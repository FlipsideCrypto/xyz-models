{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, event_type,event_address,event_module,event_resource);",
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
    event_index,
    event_type,
    event_address,
    event_module,
    event_resource,
    event_data,
    account_address,
    creation_number,
    sequence_number,
    events_id AS fact_events_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__events'
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
