{{ config(
    materialized = 'view'
) }}

SELECT
    asset_id,
    HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    hourly_prices_all_providers_id AS fact_prices_ohlc_hourly_id
FROM
    {{ ref('silver__hourly_prices_all_providers') }}
