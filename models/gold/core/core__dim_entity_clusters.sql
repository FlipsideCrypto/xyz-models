{{ config(
  materialized = 'view',
  tags = ['entity_cluster', 'core']
) }}

SELECT
  address,
  address_group,
  project_name,
  COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
  COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp,
  full_entity_cluster_id as dim_entity_clusters_id
FROM
  {{ ref('silver__full_entity_cluster_label') }}
