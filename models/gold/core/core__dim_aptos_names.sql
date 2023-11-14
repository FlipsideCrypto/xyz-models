{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    token_name,
    domain,
    domain_with_suffix,
    subdomain,
    owner_address,
    registered_address,
    is_active,
    is_primary,
    token_standard,
    expiration_timestamp,
    last_transaction_version,
    aptos_names_id AS dim_aptos_names_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__aptos_names'
    ) }}
