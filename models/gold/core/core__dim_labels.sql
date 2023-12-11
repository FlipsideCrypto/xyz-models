{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label,
    COALESCE(
        labels_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__labels') }}
