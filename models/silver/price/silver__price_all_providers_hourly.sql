{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['hour::DATE'],
    tags = ['core', 'prices']
) }}

WITH coinmarketcap AS (

    SELECT
        recorded_hour AS HOUR,
        CLOSE AS price,
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
        CLOSE AS price,
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
        CLOSE AS price,
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
    price,
    provider,
    CASE
        WHEN provider = 'coingecko' THEN 1
        WHEN provider = 'coinmarketcap' THEN 2
        WHEN provider = 'coinpaprika' THEN 3
    END AS priority,
    _inserted_timestamp
FROM
    FINAL
