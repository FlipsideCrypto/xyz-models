{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_souffl3_id",
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
        --mercato
        (
            (
                event_address = '0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4'
                AND event_resource = 'BuyTokenEvent<0x1'
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
{% else %}
    {# AND block_timestamp :: DATE >= '2022-10-19' #}
{% endif %}
),
evnts_2 AS (
    SELECT
        *,
        event_data :coin_per_token :: NUMBER AS price_raw
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
                            event_address = 'BuyTokenEvent<0x1'
                            AND event_resource = 'BuyListingEvent'
                        )
                )
            WHERE
                event_type_len > 70
                AND event_resource != 'TokenSwapEvent'
        )
),
deposit_events_souffl3 AS (
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
souffl3_add_seller_info AS (
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
                JOIN deposit_events_souffl3 seller
                ON main.tx_hash = seller.tx_hash
                AND main.event_index > seller.event_index
                AND main.prev_event_index <= seller.event_index
            WHERE
                seller.account_address != '0x37ccac35f1d5a11773d14e47786f112941eb92aaea3295a3487e1b6dc3810b2a'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                seller_ev_index
        ) = 2
),
souffl3_add_creator_fee AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                creator.event_index AS creator_ev_index,
                creator.amount AS creator_fee_raw
            FROM
                souffl3_add_seller_info main
                JOIN deposit_events_souffl3 creator
                ON main.tx_hash = creator.tx_hash
                AND main.event_index > creator.event_index
                AND main.prev_event_index <= creator.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                creator_ev_index
        ) = 1
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    'sale' AS event_type,
    event_data :buyer :: STRING AS buyer_address,
    seller_address :: STRING AS seller_address,
    event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version :: STRING AS nft_address,
    CASE
        WHEN len(nft_address) > 70 THEN 'v1'
        ELSE 'v2'
    END AS token_version,
    event_address AS platform_address,
    event_data :token_id.token_data_id.collection :: STRING AS project_name,
    event_data :token_id.token_data_id.name :: STRING AS tokenid,
    'Souffl3' AS platform_name,
    'souffl3' AS platform_exchange_version,
    price_raw AS total_price_raw,
    price_raw -(
        creator_fee_raw + amount
    ) AS platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_sales_souffl3_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    souffl3_add_creator_fee
