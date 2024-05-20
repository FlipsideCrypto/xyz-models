{{ config(
    materialized = 'incremental',
    unique_key = "full_entity_cluster_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['entity_cluster'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = False
) }}

{% if is_incremental() %}
WITH clusters_changelog AS (
    SELECT
        clusters :: ARRAY AS clusters,
        addresses :: ARRAY AS addresses,
        change_type :: STRING AS change_type,
        new_cluster_id :: BIGINT AS new_cluster_id
    FROM
        {{ target.database }}.silver.clusters_changelog

),
merges AS (
    SELECT
        s1.value :: STRING AS address_group_old,
        new_cluster_id AS address_group_new
    FROM
        clusters_changelog t,
        TABLE(FLATTEN(t.clusters)) s1
    WHERE
        t.change_type = 'merge'
),
adds_news AS (
    SELECT
        s1.value :: STRING AS address,
        t.new_cluster_id AS address_group
    FROM
        clusters_changelog t,
        TABLE(FLATTEN(t.addresses)) s1

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
)
SELECT
    address,
    address_group,
    NULL AS project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS full_entity_cluster_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    base
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
        "ENTITY_CLUSTERS_20240414_132013"
    ) }}
{% endif %}
