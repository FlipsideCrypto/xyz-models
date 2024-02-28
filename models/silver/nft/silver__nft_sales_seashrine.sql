{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_seashrine_id",
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
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            (
                event_address = '0xd5431191333a6185105c172e65f9fcd945ae92159ab648e1a9ea88c71e275548'
                AND event_resource = 'BuyListingEvent'
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
                            event_address = '0xd5431191333a6185105c172e65f9fcd945ae92159ab648e1a9ea88c71e275548'
                            AND event_resource = 'BuyListingEvent'
                        )
                )
            WHERE
                event_type_len > 70
        )
),
deposit_events_seashrine AS (
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
seashrine_add_seller_info AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                seller.account_address AS seller_address,
                seller.event_index AS seller_ev_index,
                seller.amount
            FROM
                evnts_2 main
                JOIN deposit_events_seashrine seller
                ON main.tx_hash = seller.tx_hash
                AND main.event_index > seller.event_index
                AND main.prev_event_index <= seller.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                seller_ev_index
        ) = 1
),
seashrine_add_creator_fee AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                creator.event_index AS creator_ev_index,
                creator.amount AS creator_fee_raw
            FROM
                seashrine_add_seller_info main
                JOIN deposit_events_seashrine creator
                ON main.tx_hash = creator.tx_hash
                AND main.event_index > creator.event_index
                AND main.prev_event_index <= creator.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                creator_ev_index
        ) = 2
),
seashrine_add_platform_fee AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                platform.event_index AS platform_ev_index,
                platform.amount AS platform_fee_raw
            FROM
                seashrine_add_creator_fee main
                JOIN deposit_events_seashrine platform
                ON main.tx_hash = platform.tx_hash
                AND main.event_index > platform.event_index
                AND main.prev_event_index <= platform.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                platform_ev_index
        ) = 3
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    'sale' AS event_type,
    event_data :lister :: STRING AS buyer_address,
    seller_address :: STRING AS seller_address,
    event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version :: STRING AS nft_address,
    CASE
        WHEN len(nft_address) > 70 THEN 'v1'
        ELSE 'v2'
    END AS token_version,
    event_address AS platform_address,
    event_data :token_id.token_data_id.collection :: STRING AS project_name,
    event_data :token_id.token_data_id.name :: STRING AS tokenid,
    'Seashrine' AS platform_name,
    'seashrine_market' AS platform_exchange_version,
    amount + platform_fee_raw + creator_fee_raw AS total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_sales_seashrine_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    seashrine_add_platform_fee
