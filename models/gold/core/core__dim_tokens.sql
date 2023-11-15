{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    coin_type AS token_address,
    NAME,
    symbol,
    decimals,
    coin_type_hash,
    creator_address,
    transaction_created_timestamp,
    transaction_version_created,
    coin_info_id AS dim_token_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__coin_info'
    ) }}
