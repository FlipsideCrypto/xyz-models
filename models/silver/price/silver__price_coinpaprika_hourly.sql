{{ config(
    materialized = 'incremental',
    unique_key = 'recorded_hour',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['recorded_hour ::DATE'],
    tags = ["prices", "core", "scheduled_non_core"]
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
        TIMESTAMP,
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
        DISTINCT recorded_hour,
        FIRST_VALUE(price_usd) over (
            PARTITION BY recorded_hour
            ORDER BY
                TIMESTAMP ASC rows BETWEEN unbounded preceding
                AND unbounded following
        ) AS OPEN,
        LAST_VALUE(price_usd) over (
            PARTITION BY recorded_hour
            ORDER BY
                TIMESTAMP ASC rows BETWEEN unbounded preceding
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
    '2023-10-31T13:00:00Z' AS _inserted_timestamp,
    'coinpaprika' AS provider,
    {{ dbt_utils.generate_surrogate_key(
        ['A.recorded_hour']
    ) }} AS price_coinpaprika_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    aggregated A
    JOIN windowed w
    ON A.recorded_hour = w.recorded_hour
