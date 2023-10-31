{{ config(
  materialized = 'view',
  tags = ['entity_cluster', 'core']
) }}

SELECT
  address,
  group_id,
  project_name
FROM
  {{ ref('silver__full_entity_clusters') }}
