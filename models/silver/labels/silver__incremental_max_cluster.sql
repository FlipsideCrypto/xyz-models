{{ config(
  materialized = 'view',
  tags = ['snowflake', 'cluster', 'labels', 'entity_cluster', 'incremental']
) }}

SELECT
  MAX(address_group) AS max_group_id
FROM
  {{ ref(
    "silver__full_entity_cluster"
  ) }}
