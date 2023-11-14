{{ config(
  materialized = 'view',
  tags = ['entity_cluster', 'core']
) }}

SELECT
  address,
  address_group,
  project_name,
  inserted_timestamp,
  modified_timestamp,
  full_entity_cluster_id as dim_entity_clusters_id
FROM
  {{ ref('silver__full_entity_cluster') }}
