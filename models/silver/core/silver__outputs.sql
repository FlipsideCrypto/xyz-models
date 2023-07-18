{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'output_id',
    tags = ["core"],
    cluster_by = ["tx_id", "index"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
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
FINAL AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.block_timestamp,
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
        t._partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(['t.tx_id', 'o.value :n :: NUMBER']) }} AS output_id
    FROM
        txs t,
        LATERAL FLATTEN(outputs) o
)
SELECT
    *
FROM
    FINAL
