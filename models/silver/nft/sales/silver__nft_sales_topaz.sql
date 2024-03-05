{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_topaz_id",
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
                'FillCollectionBidEvent',
                'SellEvent'
            ) THEN 'bid_won'
        END AS event_kind,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        success

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
                        event_type_len > 70
                        AND event_resource NOT IN (
                            'TokenSwapEvent',
                            'SendEvent'
                        )
                )
            WHERE
                event_address = '0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2' --types of events that indicate a sale
                AND event_resource IN (
                    'BuyEvent',
                    'FillCollectionBidEvent',
                    'SellEvent'
                )
        )
),
main_part_topaz AS (
    SELECT
        block_timestamp,
        block_number,
        version,
        tx_hash,
        prev_event_index,
        event_index,
        event_kind AS event_type,
        COALESCE(
            event_data :buyer,
            event_data :bid_buyer
        ) AS buyer_address,
        COALESCE(
            event_data :seller,
            event_data :owner,
            event_data :bid_seller
        ) AS seller_address,
        event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version AS nft_address,
        (
            CASE
                WHEN len(nft_address) > 70 THEN 'v1'
                ELSE 'v2'
            END
        ) AS token_version,
        event_address AS platform_address,
        event_data :token_id.token_data_id.collection AS project_name,
        event_data :token_id.token_data_id.name AS tokenid,
        'Topaz' AS platform_name,
        'Topaz' AS platform_exchange_version,
        event_data :price :: NUMBER AS price,
        _inserted_timestamp
    FROM
        evnts_2
),
deposit_events_topaz AS (
    SELECT
        tx_hash,
        event_index,
        event_data :amount :: NUMBER AS fee,
        account_address
    FROM
        evnts
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
topaz_main_with_creator_fees_raw AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                creator.fee,
                creator.event_index AS creator_ev_index,
                creator.account_address AS creator_royalties_address
            FROM
                main_part_topaz main
                JOIN deposit_events_topaz creator
                ON main.tx_hash = creator.tx_hash
                AND main.event_index > creator.event_index
                AND main.prev_event_index <= creator.event_index
            WHERE
                creator.account_address != '0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                creator_ev_index
        ) = 1
),
topaz_main_with_creator_fees AS (
    SELECT
        * exclude (
            fee,
            creator_ev_index,
            creator_royalties_address
        )
    FROM
        (
            SELECT
                *,
                (
                    CASE
                        WHEN creator_royalties_address = '0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2' THEN 0
                        ELSE fee
                    END
                ) AS creator_fee_raw
            FROM
                topaz_main_with_creator_fees_raw
        )
)
SELECT
    block_timestamp,
    block_number,
    version,
    main.tx_hash,
    main.event_index,
    event_type,
    seller_address :: STRING AS seller_address,
    buyer_address :: STRING AS buyer_address,
    nft_address,
    token_version,
    platform_address,
    project_name :: STRING AS project_name,
    tokenid :: STRING AS tokenid,
    platform_name,
    platform_exchange_version,
    price AS total_price_raw,
    creator_fee_raw,
    platform.fee AS platform_fee_raw,
    creator_fee_raw + platform_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['main.tx_hash','main.event_index']
    ) }} AS nft_sales_topaz_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    main._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    topaz_main_with_creator_fees main
    JOIN deposit_events_topaz platform
    ON main.tx_hash = platform.tx_hash
    AND main.event_index > platform.event_index
    AND main.prev_event_index <= platform.event_index
    AND platform.account_address = '0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2'
