{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_okx_id",
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
                event_address = '0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43'
                AND event_resource = 'OrderExecutedEvent<0x1'
            )
            OR (
                event_resource = 'DepositEvent'
                AND event_module IN (
                    'coin',
                    'token'
                )
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
                            event_address = '0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43'
                            AND event_resource = 'OrderExecutedEvent<0x1'
                        )
                )
            WHERE
                event_type_len > 70
        )
),
deposit_events_okx AS (
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
token_deposit_events_okx AS (
    SELECT
        tx_hash,
        event_index,
        event_data :id.property_version AS property_version,
        event_data :id.token_data_id.creator AS creator_address,
        event_data :id.token_data_id.collection AS project_name,
        event_data :id.token_data_id.name AS tokenid
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
        AND event_module = 'token'
),
okx_add_token_info AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                token.property_version,
                token.creator_address,
                token.event_index AS token_ev_index,
                token.project_name,
                token.tokenid
            FROM
                evnts_2 main
                JOIN token_deposit_events_okx token
                ON main.tx_hash = token.tx_hash
                AND main.event_index > token.event_index
                AND main.prev_event_index <= token.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                token_ev_index
        ) = 1
),
okx_add_seller_info AS (
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
                okx_add_token_info main
                JOIN deposit_events_okx seller
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
okx_add_creator_fee AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                creator.event_index AS creator_ev_index,
                creator.amount AS creator_fee_raw
            FROM
                okx_add_seller_info main
                JOIN deposit_events_okx creator
                ON main.tx_hash = creator.tx_hash
                AND main.event_index > creator.event_index
                AND main.prev_event_index <= creator.event_index
            WHERE
                creator.account_address != '0x40034a19a59ec81325b4d6b727cba5327733e7fff511e74d7350ce2a1ba8ac44'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                creator_fee_raw
        ) = 1
),
okx_add_platform_fee AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                platform.event_index AS platform_ev_index,
                platform.amount AS platform_fee_raw
            FROM
                okx_add_creator_fee main
                JOIN deposit_events_okx platform
                ON main.tx_hash = platform.tx_hash
                AND main.event_index > platform.event_index
                AND main.prev_event_index <= platform.event_index
            WHERE
                platform.account_address = '0x40034a19a59ec81325b4d6b727cba5327733e7fff511e74d7350ce2a1ba8ac44'
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                platform_fee_raw DESC
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
    creator_address || '::' || project_name || '::' || tokenid || '::' || property_version :: STRING AS nft_address,
    CASE
        WHEN len(nft_address) > 70 THEN 'v1'
        ELSE 'v2'
    END AS token_version,
    event_address AS platform_address,
    project_name :: STRING AS project_name,
    tokenid :: STRING AS tokenid,
    'Okx' AS platform_name,
    'okx-nft-marketplace' AS platform_exchange_version,
    event_data :executed_price :: FLOAT AS total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_sales_okx_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    okx_add_platform_fee
