{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','change_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash, change_type,inner_change_type,change_address,change_module,change_resource,payload_function);",
    tags = ['core','full_test']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    payload_function,
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

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
