{{ config(
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    NAME,
    decimals,
    price,
    FALSE AS is_native,
    is_deprecated,
    is_imputed,
    inserted_timestamp,
    modified_timestamp,
    hourly_prices_priority_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__hourly_prices_priority') }}
UNION ALL
SELECT
    HOUR,
    token_address,
    symbol,
    NAME,
    decimals,
    price,
    TRUE AS is_native,
    is_deprecated,
    is_imputed,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}
