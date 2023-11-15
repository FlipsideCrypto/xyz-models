{{ config(
    materialized = 'incremental',
    unique_key = 'clusters_changelog_store_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['entity_cluster_0'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = False
) }}

SELECT
    MD5(concat_ws('-', "addresses", SYSDATE() :: DATE)) AS clusters_changelog_store_id,
    "clusters" :: ARRAY AS CLUSTERS,
    "addresses" :: ARRAY AS addresses,
    "type" :: STRING AS change_type,
    "new_cluster_id" :: bigint AS new_cluster_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref(
        "silver__clusters_changelog"
    ) }}
