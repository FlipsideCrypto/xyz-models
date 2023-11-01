{{ config(
    materialized = 'view',
    tags = ['core', 'prices']
) }}

WITH coinmarketcap AS (

    SELECT
        recorded_hour as hour,
        CLOSE as price,
        provider
    FROM
        {{ ref('silver__price_coinmarketcap_hourly') }}
),
coingecko AS (
    SELECT
        recorded_hour as hour,
        CLOSE as price,
        provider
    FROM
        {{ ref('silver__price_coingecko_hourly') }}
),
coinpaprika AS (
    SELECT
        recorded_hour as hour,
        CLOSE as price,
        provider
    FROM
        {{ ref('silver__price_coinpaprika_hourly') }}
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
    *
FROM
    FINAL
