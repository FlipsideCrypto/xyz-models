{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'cluster', 'labels', 'entity_cluster'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT
    address,
    group_id as address_group,
    project_name,
    _inserted_timestamp
FROM
    {{ source("bitcoin_bronze", "entity_clusters") }}

