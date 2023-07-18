{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
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
        {{ ref('silver__transaction_index') }}

{% if is_incremental() %}
WHERE
    _partition_by_block_id IN (
        SELECT
            DISTINCT _partition_by_block_id
        FROM
            bronze_transactions
    )
    AND block_number IN (
        SELECT
            DISTINCT block_number
        FROM
            bronze_transactions
    )
{% endif %}
),
FINAL AS (
    SELECT
        t.block_number,
        b.block_hash,
        b.block_timestamp,
        t.tx_id,
        b.index,
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
        t._partition_by_block_id,
        t._inserted_timestamp
    FROM
        bronze_transactions t
        LEFT JOIN blocks b USING (
            block_number,
            tx_id
        )
)
SELECT
    *
FROM
    FINAL
