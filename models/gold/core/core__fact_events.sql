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
    event_index,
    event_type,
    event_address,
    event_module,
    event_resource,
    event_data,
    account_address,
    creation_number,
    sequence_number,
    _ID,
    _INSERTED_TIMESTAMP,
    _MD,
    invocation_id
FROM
    {{ ref(
        'silver__events'
    ) }}
