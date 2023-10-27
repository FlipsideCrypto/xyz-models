{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    TYPE,
    event_index,
    event_type,
    address,
    module,
    RESOURCE,
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
