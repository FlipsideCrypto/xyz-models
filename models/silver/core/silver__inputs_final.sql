{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'input_id',
    tags = ["core"],
    cluster_by = ["_inserted_timestamp::DATE", "_partition_by_block_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
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
    OR (
        _partition_by_block_id IN (
            SELECT
                DISTINCT _partition_by_block_id
            FROM
                inputs
        )
        AND block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                inputs
        )
    )
{% endif %}
),
FINAL AS (
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
        i.spent_output_index,
        o.pubkey_script_asm,
        o.pubkey_script_hex,
        o.pubkey_script_address,
        o.pubkey_script_type,
        o.pubkey_script_desc,
        o.value,
        i.tx_in_witness,
        i._inserted_timestamp,
        i._partition_by_block_id,
        i.input_id
    FROM
        inputs i
        LEFT JOIN outputs o
        ON i.spent_tx_id = o.tx_id
        AND i.spent_output_index = o.index
)
SELECT
    *
FROM
    FINAL
