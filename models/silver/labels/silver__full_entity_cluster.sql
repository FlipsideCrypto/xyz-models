{{ config(
    materialized = 'incremental',
    unique_key = "full_entity_cluster_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

{% if is_incremental() %}
WITH set_inserted_timestamp AS (
    {% if var(
            "INCREMENTAL_CLUSTER_BACKFILL",
            False
        ) %}

        SELECT
            MAX(inserted_timestamp) + INTERVAL '{{ var("INCREMENTAL_CLUSTER_INTERVAL", "24 hours") }}' AS inserted_timestamp
        FROM
            {{ this }}
        {% else %}
            SYSDATE() AS inserted_timestamp
        {% endif %}
),
merges AS (
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
    A.address,
    A.address_group,
    b.project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS full_entity_cluster_id,
    ts.inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    base A
    LEFT JOIN labs b
    ON A.address_group = b.address_group
    LEFT JOIN set_inserted_timestamp ts
    ON 1 = 1
{% else %}
SELECT
    address,
    group_id AS address_group,
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS full_entity_cluster_id,
    _inserted_timestamp AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ source(
        "bitcoin_bronze",
        "entity_clusters"
    ) }}
{% endif %}
