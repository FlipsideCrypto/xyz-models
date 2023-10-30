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
    changes_id AS fact_changes_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__changes'
    ) }}
