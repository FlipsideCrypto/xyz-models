{{ config(
  materialized = 'view',
  tags = ['snowflake', 'bitcoin', 'labels', 'cluster'],
) }}

SELECT
  *
FROM
  {{ ref('silver__entity_cluster_btc') }}
