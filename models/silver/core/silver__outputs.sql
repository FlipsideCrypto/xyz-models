{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index)"
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}

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
outputs AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        b.block_hash,
        t.tx_id AS tx_id,
        o.value :: variant AS output_data,
        o.value :n :: NUMBER AS INDEX,
        o.value :scriptPubKey :address :: STRING AS address,
        o.value :scriptPubKey :asm :: STRING AS asm,
        o.value :scriptPubKey :desc :: STRING AS DESC,
        o.value :scriptPubKey :hex :: STRING AS hex,
        o.value :scriptPubKey :type :: STRING AS TYPE,
        o.value :value :: FLOAT AS VALUE,
        concat_ws('-', t.tx_id, o.value :n :: STRING) AS output_id,
        t._inserted_timestamp,
        t._partition_by_block_id
    FROM
        txs t
        LEFT JOIN blocks b USING (block_number),
        LATERAL FLATTEN(outputs) o
)
SELECT
    *
FROM
    outputs
