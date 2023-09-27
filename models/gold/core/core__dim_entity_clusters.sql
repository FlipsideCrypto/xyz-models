{{ config(
  materialized = 'view',
  tags = ['entity_cluster', 'core']
) }}

SELECT
  address,
  address_group,
  project_name
FROM
  {{ ref('silver__entity_cluster_btc') }}
