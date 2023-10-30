{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    epoch,
    events,
    changes,
    failed_proposer_indices,
    id,
    previous_block_votes_bitvec,
    proposer,
    ROUND,
    vm_status,
    state_change_hash,
    accumulator_root_hash,
    event_root_hash,
    transactions_id AS fact_transactions_block_metadata_id,
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
    ) = 'block'
