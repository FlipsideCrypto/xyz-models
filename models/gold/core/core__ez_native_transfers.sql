{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    from_address,
    to_address,
    amount,
    token_address,
    transfers_native_id AS ez_native_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__transfers_native'
    ) }}
