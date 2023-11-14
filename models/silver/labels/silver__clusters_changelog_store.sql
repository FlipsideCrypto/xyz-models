{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', addresses, _INSERTED_DATE)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'cluster', 'labels', 'entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = False
) }}

SELECT
    "clusters" AS CLUSTERS,
    "addresses" AS addresses,
    "type" AS change_type,
    "new_cluster_id" AS new_cluster_id,
    CURRENT_TIMESTAMP AS _INSERTED_DATE
FROM
    {{ ref(
        "silver__clusters_changelog"
    ) }}
