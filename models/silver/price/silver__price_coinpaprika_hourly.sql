{{ config(
    materialized = 'incremental',
    unique_key = 'recorded_hour',
    incremental_strategy='delete+insert',
    cluster_by = ['recorded_hour ::DATE'],
    tags = ['prices']
) }}

WITH prices AS (

    SELECT
        TIMESTAMP :: timestamp_ntz AS TIMESTAMP,
        price_usd
    FROM
        {{ source(
            'streamline_crosschain',
            'coin_paprika_btc'
        ) }}

{% if is_incremental() %}
WHERE
    TIMESTAMP > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
prices_hourly AS (
    SELECT
        DATE_TRUNC(
            'hour',
            TIMESTAMP
        ) :: timestamp_ntz AS recorded_hour,
        price_usd
    FROM
        prices
),
aggregated AS (
    SELECT
        recorded_hour,
        MAX(price_usd) AS high,
        MIN(price_usd) AS low
    FROM
        prices_hourly
    GROUP BY
        1
),
windowed AS (
    SELECT
        recorded_hour,
        FIRST_VALUE(price_usd) over (
            PARTITION BY recorded_hour
            ORDER BY
                recorded_hour ASC rows BETWEEN unbounded preceding
                AND unbounded following
        ) AS OPEN,
        LAST_VALUE(price_usd) over (
            PARTITION BY recorded_hour
            ORDER BY
                recorded_hour ASC rows BETWEEN unbounded preceding
                AND unbounded following
        ) AS CLOSE
    FROM
        prices_hourly
)
SELECT
    A.recorded_hour,
    w.open,
    A.high,
    A.low,
    w.close,
    '2023-10-31T13:00:00Z' as _inserted_timestamp,
    'coinpaprika' as provider
FROM
    aggregated A
    JOIN windowed w
    ON A.recorded_hour = w.recorded_hour
