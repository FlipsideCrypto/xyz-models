{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['hour::DATE'],
    tags = ["prices", "core", "scheduled_non_core"]
) }}

WITH coinmarketcap AS (

    SELECT
        recorded_hour AS HOUR,
        OPEN,
        high,
        low,
        CLOSE,
        provider,
        _inserted_timestamp
    FROM
        {{ ref('silver__price_coinmarketcap_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
coingecko AS (
    SELECT
        recorded_hour AS HOUR,
        OPEN,
        high,
        low,
        CLOSE,
        provider,
        _inserted_timestamp
    FROM
        {{ ref('silver__price_coingecko_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
coinpaprika AS (
    SELECT
        recorded_hour AS HOUR,
        OPEN,
        high,
        low,
        CLOSE,
        provider,
        _inserted_timestamp
    FROM
        {{ ref('silver__price_coinpaprika_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        *
    FROM
        coinmarketcap
    UNION ALL
    SELECT
        *
    FROM
        coingecko
    UNION ALL
    SELECT
        *
    FROM
        coinpaprika
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['hour', 'provider']
    ) }} AS id,
    HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    CASE
        WHEN provider = 'coingecko' THEN 1
        WHEN provider = 'coinmarketcap' THEN 2
        WHEN provider = 'coinpaprika' THEN 3
    END AS priority,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['hour', 'provider']
    ) }} AS price_all_providers_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
