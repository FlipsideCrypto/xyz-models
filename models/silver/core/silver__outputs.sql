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
final AS (
    SELECT
        t.block_number,
        b.block_timestamp,
        b.block_hash,
        t.tx_id AS tx_id,
        o.value :: variant AS output_data,
        o.value :n :: NUMBER AS INDEX,
        o.value :scriptPubKey :address :: STRING AS pubkey_script_address,
        o.value :scriptPubKey :asm :: STRING AS pubkey_script_asm,
        o.value :scriptPubKey :desc :: STRING AS pubkey_script_desc,
        o.value :scriptPubKey :hex :: STRING AS pubkey_script_hex,
        o.value :scriptPubKey :type :: STRING AS pubkey_script_type,
        o.value :value :: FLOAT AS VALUE,
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
    final
