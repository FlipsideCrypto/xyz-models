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
    event_index,
    creation_number,
    transfer_event,
    account_address,
    amount,
    token_address,
    transfers_id AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__transfers'
    ) }}
