{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_bluemove_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH evnts AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        account_address,
        event_address,
        event_resource,
        event_data,
        event_module,
        event_type,
        CASE
            WHEN event_resource = 'BuyEvent' THEN 'sale'
            WHEN event_resource IN (
                'AcceptTokenBidEvent',
                'AcceptCollectionBidEvent'
            ) THEN 'bid_won'
        END AS event_kind,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            (
                event_address = '0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e' --types of events that indicate a sale
                AND event_resource IN (
                    'BuyEvent',
                    'AcceptTokenBidEvent',
                    'AcceptCollectionBidEvent'
                )
            )
            OR (
                event_resource = 'DepositEvent'
                AND event_module = 'coin'
            )
        )
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
evnts_2 AS (
    SELECT
        *
    FROM
        (
            SELECT
                *,
                LAG(
                    event_index,
                    1,
                    0
                ) over (
                    PARTITION BY tx_hash
                    ORDER BY
                        event_index
                ) AS prev_event_index
            FROM
                (
                    SELECT
                        *,
                        len(event_type) AS event_type_len
                    FROM
                        evnts
                    WHERE
                        (
                            event_address = '0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e' --types of events that indicate a sale
                            AND event_resource IN (
                                'BuyEvent',
                                'AcceptTokenBidEvent',
                                'AcceptCollectionBidEvent'
                            )
                        )
                )
            WHERE
                event_type_len > 70
        )
),
deposit_event_bluemove AS (
    SELECT
        tx_hash,
        event_index,
        event_data :amount :: NUMBER AS amount,
        account_address
    FROM
        evnts A
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                evnts_2
        )
        AND event_resource = 'DepositEvent'
        AND event_module = 'coin'
),
bluemove_add_seller_info AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                seller.account_address AS seller_address,
                seller.amount
            FROM
                evnts_2 main
                JOIN deposit_event_bluemove seller
                ON main.tx_hash = seller.tx_hash
                AND main.event_index > seller.event_index
                AND main.prev_event_index <= seller.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                amount DESC
        ) = 1
),
bluemove_add_creator_fee AS (
    SELECT
        *,
        (
            CASE
                WHEN amount = creator_fee_raw1 THEN 0
                ELSE creator_fee_raw1
            END
        ) AS creator_fee_raw
    FROM
        (
            SELECT
                *
            FROM
                (
                    SELECT
                        main.*,
                        creator.account_address AS creator_address,
                        creator.amount AS creator_fee_raw1
                    FROM
                        bluemove_add_seller_info main
                        JOIN deposit_event_bluemove creator
                        ON main.tx_hash = creator.tx_hash
                        AND main.event_index > creator.event_index
                        AND main.prev_event_index <= creator.event_index
                    WHERE
                        creator.account_address != '0x6fa430a236dcae28b60bfd93d5a96f2225e0357bd1542b2b8da4a430bdceed64'
                ) qualify ROW_NUMBER() over (
                    PARTITION BY tx_hash,
                    event_index
                    ORDER BY
                        creator_fee_raw1
                ) = 1
        )
),
bluemove_add_platform_fee_raw AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                platform.account_address AS platform_address,
                platform.amount AS platform_fee_raw
            FROM
                bluemove_add_creator_fee main
                JOIN deposit_event_bluemove platform
                ON main.tx_hash = platform.tx_hash
                AND main.event_index > platform.event_index
                AND main.prev_event_index <= platform.event_index
            WHERE
                platform.account_address = '0x6fa430a236dcae28b60bfd93d5a96f2225e0357bd1542b2b8da4a430bdceed64'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                platform_fee_raw DESC
        ) = 1
),
bluemove_add_platform_fee AS (
    SELECT
        *
    FROM
        bluemove_add_platform_fee_raw
    UNION
    SELECT
        *,
        '0x6fa430a236dcae28b60bfd93d5a96f2225e0357bd1542b2b8da4a430bdceed64' AS platform_address,
        0 AS platform_fee_raw
    FROM
        bluemove_add_creator_fee
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                bluemove_add_platform_fee_raw
        )
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_kind AS event_type,
    COALESCE(
        event_data :buyer_address,
        event_data :offer_collection_item.offerer,
        event_data :offerer
    ) :: STRING AS buyer_address,
    seller_address :: STRING AS seller_address,
    COALESCE(
        event_data :id.token_data_id.creator || '::' || event_data :id.token_data_id.collection || '::' || event_data :id.token_data_id.name || '::' || event_data :id.property_version,
        event_data :token_metadata.token.vec [0].inner,
        event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version
    ) :: STRING AS nft_address,
    CASE
        WHEN len(nft_address) > 70 THEN 'v1'
        ELSE 'v2'
    END AS token_version,
    event_address AS platform_address,
    COALESCE(
        event_data :id.token_data_id.collection,
        event_data :token_id.token_data_id.collection
    ) :: STRING AS project_name,
    COALESCE(
        event_data :id.token_data_id.name,
        event_data :token_id.token_data_id.name
    ) :: STRING AS tokenid,
    'BlueMove' AS platform_name,
    'Marketplace' AS platform_exchange_version,
    amount + platform_fee_raw + creator_fee_raw AS total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_sales_bluemove_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bluemove_add_platform_fee
