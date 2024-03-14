{{ config(
    materialized = 'incremental',
    unique_key = 'output_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["_partition_by_address_group", "modified_timestamp::DATE"]
) }}

WITH CLUSTERS AS (

    SELECT
        address,
        address_group,
        FLOOR(
            address_group,
            -3
        ) AS _partition_by_address_group,
        -- might want change_type as well tbd
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__full_entity_cluster') }}
        -- import incrementally
),
outputs AS (
    SELECT
        tx_id,
        output_id,
        block_number,
        block_timestamp,
        pubkey_script_address,
        VALUE,
        _partition_by_block_id,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__outputs') }}
        -- import incrementally
),
append_group AS (
    SELECT
        o.tx_id,
        o.output_id,
        o.block_number,
        o.block_timestamp,
        o.pubkey_script_address,
        o.value,
        C.address_group,
        IFF(
            C._partition_by_address_group IS NULL,
            0,
            _partition_by_address_group
        ) AS _partition_by_address_group,
        o._partition_by_block_id,
        GREATEST(
            o._modified_timestamp,
            C._modified_timestamp
        ) AS _modified_timestamp
    FROM
        outputs o
        LEFT JOIN CLUSTERS C
),
{% if is_incremental() %}
update_modified_cluster AS (
    SELECT
        t.tx_id,
        t.output_id,
        t.block_number,
        t.block_timestamp,
        t.pubkey_script_address,
        t.value,
        C.address_group,
        C._partition_by_address_group,
        t._partition_by_block_id,
        _modified_timestamp
    FROM
        {{ this }}
        t
        LEFT JOIN CLUSTERS C
        ON t.pubkey_script_address = C.address
    WHERE
        _partition_by_address_group IN (
            SELECT
                DISTINCT _partition_by_address_group
            FROM
                CLUSTERS
        )
        AND C.address_group is not null

),
{% endif %}
FINAL as (
    SELECT
        *
    FROM
        append_group
{% if is_incremental() %}
    UNION ALL
    SELECT
        *
    FROM
        update_modified_cluster
{% endif %}
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
