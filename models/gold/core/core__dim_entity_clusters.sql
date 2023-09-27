{{ config(
  materialized = 'view',
  tags = ['entity_cluster', 'core']
) }}

SELECT
  *
FROM
  {{ ref('silver__entity_cluster_btc') }}
