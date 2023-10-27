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
    vm_status,
    state_checkpoint_hash,
    accumulator_root_hash,
    event_root_hash,
    _ID,
    _INSERTED_TIMESTAMP,
    _MD,
    invocation_id
FROM
    {{ ref(
        'silver__transactions'
    ) }}
WHERE
    LEFT(
        tx_type,
        5
    ) = 'state'
