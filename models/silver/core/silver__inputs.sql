{{ config(
    materialized = 'incremental',
    unique_key = 'input_id',
    tags = ["core"]
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
outputs AS (
    SELECT
        *
    FROM
        {{ ref('silver__outputs') }}

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
        t.block_hash,
        t.block_timestamp,
        t.tx_id AS tx_id,
        i.value :: variant AS input_data,
        i.index AS INDEX,
        i.value :scriptSig :asm :: STRING AS script_sig_asm,
        i.value :scriptSig :hex :: STRING AS script_sig_hex,
        i.value :sequence :: NUMBER AS SEQUENCE,
        i.value :txid :: STRING AS spent_tx_id,
        i.value :vout :: NUMBER AS spent_output_index,
        i.value :txinwitness :: ARRAY AS tx_in_witness,
        i.value :coinbase :: STRING AS coinbase,
        coinbase IS NOT NULL AS is_coinbase,
        t._inserted_timestamp,
        t._partition_by_block_id
    FROM
        txs t,
        LATERAL FLATTEN(inputs) i
),
final AS (
    SELECT
        i.block_number,
        i.block_timestamp,
        i.block_hash,
        i.tx_id,
        i.input_data,
        i.index,
        i.is_coinbase,
        i.coinbase,
        i.script_sig_asm,
        i.script_sig_hex,
        i.sequence,
        o.block_number AS spent_block_number,
        i.spent_tx_id,
        i.spent_output_index, -- TODO note will be null for coinbase
        o.pubkey_script_asm,
        o.pubkey_script_hex,
        o.pubkey_script_address,
        o.pubkey_script_type,
        o.value,
        i.tx_in_witness,
        LEAST(
            i._inserted_timestamp,
            o._inserted_timestamp
        ) AS _inserted_timestamp,
        i._partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(['i.tx_id', 'i.index']) }} as input_id
    FROM
        inputs i
        LEFT JOIN outputs o
        ON i.spent_tx_id = o.tx_id
        AND i.spent_output_index = o.index
)
SELECT
    *
FROM
    final
