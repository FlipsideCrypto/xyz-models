{{ config(
    materialized = 'incremental',
    unique_key = 'input_id',
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
-- some sort of
inputs_final AS (
    SELECT
        tx_id,
        input_id,
        block_number,
        block_timestamp,
        pubkey_script_address,
        _partition_by_block_id,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref(
            'silver__inputs_final'
        ) }}
        -- import incrementally
),
append_group AS (
    SELECT
        i.tx_id,
        i.input_id,
        i.block_number,
        i.block_timestamp,
        i.pubkey_script_address,
        C.address_group,
        IFF(
            C._partition_by_address_group IS NULL,
            0,
            _partition_by_address_group
        ) AS _partition_by_address_group,
        i._partition_by_block_id,
        GREATEST(
            i._modified_timestamp,
            C._modified_timestamp
        ) AS _modified_timestamp
    FROM
        inputs_final i
        LEFT JOIN CLUSTERS C
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    append_group
