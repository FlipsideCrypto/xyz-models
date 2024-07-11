{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_native_prices_id',
    cluster_by = ['HOUR::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH xchain_complete_prices AS (

    SELECT
        HOUR,
        asset_id,
        symbol,
        NAME,
        decimals,
        price,
        blockchain,
        is_imputed,
        is_deprecated,
        provider,
        source,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp,
        complete_native_prices_id,
        _invocation_id
    FROM
        {{ ref(
            'bronze__complete_native_prices'
        ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
coinpaprika AS (
    SELECT
        recorded_hour AS HOUR,
        NULL AS asset_id,
        'BTC' AS symbol,
        'Bitcoin' AS NAME,
        8 AS decimals,
        CLOSE AS price,
        'bitcoin' AS blockchain,
        FALSE AS is_imputed,
        TRUE AS is_deprecated,
        provider,
        'history' AS source,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp,
        price_coinpaprika_hourly_id AS complete_native_prices_id,
        _invocation_id
    FROM
        {{ ref('silver__price_coinpaprika_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    xchain_complete_prices
UNION ALL
SELECT
    *
FROM
    coinpaprika
