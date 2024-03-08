{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["_partition_by_block_id", "tx_id"],
    tags = ["core", "scheduled_core"]
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_hash,
        block_timestamp,
        VALUE :: STRING AS tx_id,
        INDEX,
        _inserted_timestamp,
        _partition_by_block_id
    FROM
        blocks,
        LATERAL FLATTEN(tx)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transaction_index_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
