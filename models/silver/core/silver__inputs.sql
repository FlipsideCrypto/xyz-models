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
inputs AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        b.block_hash,
        t.tx_id AS tx_id,
        i.index AS INDEX,
        i.value :: variant AS input_data,
        i.value :scriptSig :asm :: STRING AS script_sig_asm,
        i.value :scriptSig :hex :: STRING AS script_sig_hex,
        i.value :sequence :: NUMBER AS SEQUENCE,
        i.value :txid :: STRING AS spent_tx_id,
        i.value :vout :: NUMBER AS output_index,
        i.value :txinwitness :: ARRAY AS tx_in_witness,
        i.value :coinbase :: STRING AS coinbase,
        coinbase IS NOT NULL AS is_coinbase,
        concat_ws('-', i.value :txid :: STRING, i.value :vout :: STRING) as prior_output_id,
        t._inserted_timestamp,
        t._partition_by_block_id
    FROM
        txs t
        LEFT JOIN blocks b USING (block_number),
        LATERAL FLATTEN(inputs) i
)
SELECT
    *
FROM
    inputs
