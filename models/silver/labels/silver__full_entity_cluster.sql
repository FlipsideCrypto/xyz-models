{{ config(
    materialized = 'incremental',
    unique_key = "full_entity_cluster_id",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'cluster', 'labels', 'entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = False
) }}

{% if is_incremental() %}
WITH merges AS (

    SELECT
        s1.value :: STRING AS address_group_old,
        "new_cluster_id" AS address_group_new
    FROM
        {{ target.database }}.silver.clusters_changelog t,
        TABLE(FLATTEN(t."clusters")) s1
    WHERE
        t."type" IN ('merge')
),
adds_news AS (
    SELECT
        s1.value :: STRING AS address,
        t."new_cluster_id" AS address_group
    FROM
        {{ target.database }}.silver.clusters_changelog t,
        TABLE(FLATTEN(t."addresses")) s1
    WHERE
        t."type" IN (
            'new',
            'addition'
        )
),
adds_news_full AS (
    SELECT
        address,
        COALESCE(
            me.address_group_new,
            address_group
        ) AS address_group
    FROM
        adds_news t
        LEFT JOIN merges me
        ON me.address_group_old = t.address_group
),
merges_full AS (
    SELECT
        t.address,
        me.address_group_new AS address_group
    FROM
        {{ this }}
        t
        INNER JOIN merges me
        ON me.address_group_old = t.address_group
),
base AS (
    SELECT
        *
    FROM
        adds_news_full
    UNION
    SELECT
        *
    FROM
        merges_full
),
labs AS (
    SELECT
        DISTINCT address_group,
        project_name
    FROM
        base A
        LEFT JOIN (
            SELECT
                DISTINCT address,
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
        project_name IS NOT NULL
    UNION
    SELECT
        DISTINCT address_group,
        project_name
    FROM
        {{ this }}
    WHERE
        address_group IN (
            SELECT
                DISTINCT address_group
            FROM
                base
        )
)
SELECT
    DISTINCT A.address,
    A.address_group,
    b.project_name,
    CURRENT_TIMESTAMP AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }} AS full_entity_cluster_id,
    CURRENT_TIMESTAMP AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id

FROM
    base A
    LEFT JOIN labs b
    ON A.address_group = b.address_group
{% else %}
SELECT
    address,
    group_id AS address_group,
    project_name,
    _inserted_timestamp
FROM
    {{ source(
        "bitcoin_bronze",
        "entity_clusters"
    ) }}
{% endif %}
