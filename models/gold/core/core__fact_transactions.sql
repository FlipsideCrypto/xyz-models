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
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }}
