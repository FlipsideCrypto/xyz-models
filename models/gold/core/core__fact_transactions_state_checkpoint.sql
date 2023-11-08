{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    vm_status,
    state_checkpoint_hash,
    accumulator_root_hash,
    event_root_hash,
    transactions_id AS fact_transactions_state_checkpoint_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }}
WHERE
    LEFT(
        tx_type,
        5
    ) = 'state'
