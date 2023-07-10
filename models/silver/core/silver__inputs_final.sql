{{ config(
    materialized = 'incremental',
    unique_key = "concat_ws('-', tx_id, index)"
) }}

WITH inputs AS (

    SELECT
        *
    FROM
        {{ ref('silver__inputs') }}

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
inputs_final AS (
    SELECT
        i.block_number,
        i.block_timestamp,
        i.block_hash,
        i.tx_id,
        i.index,
        i.spent_tx_id,
        i.output_index,
        o.address,
        o.type,
        o.value,
        i.script_sig_asm,
        i.script_sig_hex,
        i.sequence,
        i.tx_in_witness,
        i.coinbase,
        i.is_coinbase,
        LEAST(
            i._inserted_timestamp,
            o._inserted_timestamp
        ) AS _inserted_timestamp,
        i._partition_by_block_id
    FROM
        inputs i
        LEFT JOIN outputs o
        ON i.spent_tx_id = o.tx_id
            AND i.output_index = o.index
)
SELECT
    *
FROM
    inputs_final
