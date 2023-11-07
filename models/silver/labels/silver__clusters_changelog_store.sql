{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', addresses, _INSERTED_DATE)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'cluster', 'labels', 'entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}
-- We do not want to full refresh this model
-- to full-refresh either include the variable allow_full_refresh: True to command or comment out below code
-- DO NOT FORMAT will break the full refresh code if formatted copy from below
-- {% if execute %}
--   {% if flags.FULL_REFRESH and var('allow_full_refresh', False) != True %}
--       {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
--   {% endif %}
-- {% endif %}
{% if execute %}
    {% if flags.full_refresh and var(
            'allow_full_refresh',
            False
        ) != True %}
        {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
    {% endif %}
{% endif %}


SELECT
    "clusters" as CLUSTERS,
    "addresses" as ADDRESSES,
    "type" as CHANGE_TYPE,
    "new_cluster_id" as NEW_CLUSTER_ID,
    CURRENT_TIMESTAMP :: date as _INSERTED_DATE
FROM
    {{ ref(
        "silver__clusters_changelog"
    ) }}
