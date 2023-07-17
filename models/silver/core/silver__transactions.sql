{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"],
    tags = ["core"]
) }}

WITH bronze_transactions AS (

    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}

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
blocks AS (
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
compute_tx_index AS (
    SELECT
        INDEX,
        VALUE AS tx_id
    FROM
        blocks,
        LATERAL FLATTEN(tx)
),
FINAL AS (
    SELECT
        t.block_number,
        b.block_hash,
        b.block_timestamp,
        tx_id,
        i.index,
        DATA :hash :: STRING AS tx_hash,
        DATA :hex :: STRING AS hex,
        DATA :locktime :: STRING AS lock_time,
        DATA :size :: NUMBER AS SIZE,
        DATA :version :: NUMBER AS version,
        DATA :vin :: ARRAY AS inputs,
        ARRAY_SIZE(inputs) AS input_count,
        DATA :vout :: ARRAY AS outputs,
        ARRAY_SIZE(outputs) AS output_count,
        DATA :vsize :: STRING AS virtual_size,
        DATA :weight :: STRING AS weight,
        DATA: fee :: FLOAT AS fee,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        bronze_transactions t
        LEFT JOIN compute_tx_index i USING (tx_id)
        LEFT JOIN blocks b using (block_number)
)
SELECT
    *
FROM
    FINAL
