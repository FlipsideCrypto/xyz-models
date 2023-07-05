{{ config(
    materialized = 'incremental',
    unique_key = 'block_number',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"]
) }}
-- depends on {{ref('bronze__streamline_blocks')}}
WITH bronze_blocks AS (

    SELECT
        *
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}
),
blocks AS (
    SELECT
        block_number,
        DATA :result :bits :: STRING AS bits,
        DATA :result :chainwork :: STRING AS chainwork,
        DATA :result :difficulty :: FLOAT AS difficulty,
        DATA :result :hash :: STRING AS block_hash,
        DATA :result :mediantime :: STRING AS mediantime_epoch,
        DATA :result :mediantime :: timestamp_ntz AS mediantime,
        DATA :result :merkleroot :: STRING AS merkle_root,
        DATA :result :nTx :: NUMBER AS tx_count,
        DATA :result :nextblockhash :: STRING AS next_block_hash,
        DATA :result :nonce :: NUMBER AS nonce,
        DATA :result :previousblockhash :: STRING AS previous_block_hash,
        DATA :result :strippedsize :: NUMBER AS stripped_size,
        DATA :result :size :: NUMBER AS SIZE,
        DATA :result :time :: STRING AS block_timestamp_epoch,
        DATA :result :time :: timestamp_ntz AS block_timestamp,
        DATA :result :tx :: ARRAY AS tx,
        DATA :result :version :: STRING AS version,
        DATA :result :versionHex :: STRING AS version_hex,
        DATA :result :weight :: STRING AS weight,
        DATA: error :: STRING AS error,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        bronze_blocks
)
SELECT
    *
FROM
    blocks
