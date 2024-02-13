

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"]
) }}


WITH changelog AS (
  SELECT
    *
  FROM
    {{ ref('silver__clusters_changelog') }}
  WHERE

),
transfers AS (
  SELECT
    *
  FROM
    {{ ref('silver__transfer') }}
  WHERE
  
),
additions as (
-- we get the transfers in cluster 0
-- we get the addtions in change log
-- we flatten the addresses with their cluster
-- we update transfers with their new cluster based on the addresses 
),
merge as (
    -- get transfers in the cluster
    -- update the cluster from or to , to the new
),
new as (
    -- get the additions in the cluster
    -- update the cluster from or to , to the new
)

-- new and merge, uncluster address joining with the cluster
