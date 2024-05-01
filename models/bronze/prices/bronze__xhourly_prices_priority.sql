{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    HOUR,
    token_address,
    blockchain,
    price,
    is_imputed,
    _inserted_timestamp,
    modified_timestamp,
    inserted_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'token_prices_priority_hourly'
    ) }}
    {# WHERE
    blockchain = 'aptos'
    AND token_address LIKE '%:%' #}
