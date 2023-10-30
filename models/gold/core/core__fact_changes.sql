{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_type,
    change_index,
    change_data,
    change_type,
    address,
    handle,
    inner_change_type,
    change_address,
    change_module,
    change_resource,
    key,
    VALUE,
    state_key_hash,
    _ID,
    _INSERTED_TIMESTAMP,
    _MD,
    invocation_id
FROM
    {{ ref(
        'silver__changes'
    ) }}
