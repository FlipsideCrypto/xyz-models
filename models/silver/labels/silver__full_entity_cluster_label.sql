{{ config(
    materialized = 'incremental',
    unique_key = "full_entity_cluster_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH labeled_groups AS(

    SELECT
        DISTINCT address_group,
        b.project_name
    FROM
        {{ ref('silver__full_entity_cluster') }} A
        LEFT JOIN (
            SELECT
                DISTINCT address AS address,
                project_name
            FROM
                {{ source(
                    "crosschain",
                    "dim_labels"
                ) }}
            WHERE
                blockchain = 'bitcoin'
                AND label_type != 'nft'
        ) b
        ON A.address = b.address
    WHERE
        b.project_name IS NOT NULL
)
SELECT
    A.address,
    A.address_group,
    b.project_name,
    A.full_entity_cluster_id,
    A.inserted_timestamp,
    A.modified_timestamp,
    A.invocation_id
FROM
    {{ ref('silver__full_entity_cluster') }} A
    LEFT JOIN labeled_groups b
    ON A.address_group = b.address_group
