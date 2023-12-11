{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'block_number',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"],
    tags = ["core", "scheduled_core"]
) }}

WITH bronze_blocks AS (

    SELECT
        *
    FROM
        {{ ref('bronze__blocks') }}

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
        DATA :result :bits :: STRING AS bits,
        DATA :result :chainwork :: STRING AS chainwork,
        DATA :result :difficulty :: FLOAT AS difficulty,
        DATA :result :hash :: STRING AS block_hash,
        DATA :result :mediantime :: timestamp_ntz AS median_time,
        DATA :result :merkleroot :: STRING AS merkle_root,
        DATA :result :nTx :: NUMBER AS tx_count,
        DATA :result :nextblockhash :: STRING AS next_block_hash,
        DATA :result :nextblockhash IS NULL AS is_pending,
        DATA :result :nonce :: NUMBER AS nonce,
        DATA :result :previousblockhash :: STRING AS previous_block_hash,
        DATA :result :strippedsize :: NUMBER AS stripped_size,
        DATA :result :size :: NUMBER AS SIZE,
        DATA :result :time :: timestamp_ntz AS block_timestamp,
        DATA :result :tx :: ARRAY AS tx,
        DATA :result :version :: STRING AS version,
        DATA :result :weight :: STRING AS weight,
        DATA: error :: STRING AS error,
        _partition_by_block_id,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }} AS blocks_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        bronze_blocks
)
SELECT
    *
FROM
    FINAL
