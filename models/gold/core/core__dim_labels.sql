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
    project_name AS label
FROM
    {{ ref('silver__labels') }}
