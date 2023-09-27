{{ config(
    materialized = 'view',
    tags = ['entity_cluster']
) }}

SELECT
    DISTINCT LOWER(address) AS address,
    project_name
FROM
    {{ ref("silver__labels") }}
WHERE
    label_type != 'nft'
