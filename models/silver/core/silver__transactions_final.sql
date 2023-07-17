{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"],
    tags = ["core"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH transactions AS (

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
inputs AS (
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
input_val AS (
    SELECT
        tx_id,
        is_coinbase,
        SUM(VALUE) AS input_value
    FROM
        inputs
    GROUP BY
        1,
        2
),
output_val AS (
    SELECT
        tx_id,
        SUM(VALUE) AS output_value
    FROM
        outputs
    GROUP BY
        1
),
transactions_final AS (
    SELECT
        block_number,
        block_hash,
        block_timestamp,
        t.tx_id,
        INDEX,
        tx_hash,
        hex,
        lock_time,
        SIZE,
        version,
        i.is_coinbase,
        ii.coinbase,
        inputs,
        input_count,
        i.input_value,
        outputs,
        output_count,
        o.output_value,
        virtual_size,
        weight,
        IFF(
            is_coinbase,
            0,
            fee
        ) AS fee,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        transactions t
        LEFT JOIN input_val i USING (tx_id)
        LEFT JOIN output_val o USING (tx_id)
        LEFT JOIN inputs USING (tx_id) ii
)
SELECT
    *
FROM
    transactions_final
