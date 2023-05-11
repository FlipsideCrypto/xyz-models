{{ config(
    materialized = 'table'
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name AS address_name,
    NULL AS raw_metadata
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'evmos'

UNION 
SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    label AS address_name,
    raw_metadata
FROM
    {{ ref('core__dim_tokens') }}
WHERE
    blockchain = 'evmos'
UNION 
SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    label AS address_name,
    NULL AS raw_metadata
FROM
    {{ ref('core__fact_validators') }}
WHERE
    blockchain = 'evmos' 