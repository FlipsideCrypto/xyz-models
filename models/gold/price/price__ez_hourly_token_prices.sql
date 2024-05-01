{{ config(
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address_lower AS token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ ref('silver__hourly_prices_priority') }}
UNION ALL
SELECT
    HOUR,
    '0x1::aptos_coin::aptoscoin' AS token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ ref('silver__complete_native_prices') }}
