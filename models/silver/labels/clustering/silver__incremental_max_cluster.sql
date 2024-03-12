{{ config(
  materialized = 'view',
  tags = ['entity_cluster_0']
) }}

SELECT
  MAX(address_group) AS max_group_id
FROM
  {{ ref(
    "silver__full_entity_cluster"
  ) }}
