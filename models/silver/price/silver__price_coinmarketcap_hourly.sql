{{ config(
    materialized = 'incremental',
    unique_key = 'recorded_hour',
    cluster_by = ['recorded_hour ::DATE'],
    tags = ['prices']
) }}

WITH prices AS (

    SELECT
        *
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_market_cap'
        ) }}
    WHERE
        id = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    recorded_hour :: timestamp_ntz AS recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    volume,
    market_cap,
    'coinmarketcap' AS provider,
    _inserted_timestamp
FROM
    prices
