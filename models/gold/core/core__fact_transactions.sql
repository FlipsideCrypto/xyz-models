{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_TYPE,
    sender,
    signature,
    payload,
    changes,
    events,
    gas_unit_price,
    gas_used,
    max_gas_amount,
    expiration_timestamp_secs,
    vm_status,
    state_change_hash,
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
