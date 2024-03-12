{{ config(
    materialized = 'table',
    unique_key = "full_entity_cluster_id",
    tags = ['entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH labels AS (

    SELECT
        *
    FROM
        {{ source(
            "crosschain",
            "dim_labels"
        ) }}
    WHERE
        blockchain = 'bitcoin'
        AND label_type != 'nft'
),
CLUSTERS AS (
    SELECT
        *
    FROM
        {{ ref('silver__full_entity_cluster') }}
),
labeled_groups AS(
    SELECT
        A.address,
        address_group,
        b.project_name,
        full_entity_cluster_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id,
        b.insert_date AS inserted_timestamp_label
    FROM
        CLUSTERS A
        LEFT JOIN labels b
        ON A.address = b.address
)
SELECT
    *
FROM
    labeled_groups
