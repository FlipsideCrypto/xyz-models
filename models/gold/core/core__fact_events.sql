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
