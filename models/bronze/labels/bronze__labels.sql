{{ config(
    materialized = 'view',
    tags = ['core', 'scheduled_non_core']

) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    source,
    _is_deleted,
    labels_combined_id,
    modified_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'bitcoin'
