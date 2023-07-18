{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
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
    OR (
        _partition_by_block_id IN (
            SELECT
                DISTINCT _partition_by_block_id
            FROM
                blocks
        )
        AND block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                blocks
        )
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
                blocks
        )
        AND block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                blocks
        )
    )
{% endif %}
),
input_val AS (
    SELECT
        block_number,
        tx_id,
        SUM(VALUE) AS input_value
    FROM
        inputs
    GROUP BY
        1,
        2
),
output_val AS (
    SELECT
        block_number,
        tx_id,
        SUM(VALUE) AS output_value
    FROM
        outputs
    GROUP BY
        1,
        2
),
coinbase AS (
    SELECT
        block_number,
        tx_id,
        is_coinbase,
        coinbase
    FROM
        inputs
    WHERE
        is_coinbase
),
transactions_final AS (
    SELECT
        t.block_number,
        block_hash,
        block_timestamp,
        t.tx_id,
        INDEX,
        tx_hash,
        hex,
        lock_time,
        SIZE,
        version,
        COALESCE(
            C.is_coinbase,
            FALSE
        ) AS is_coinbase,
        C.coinbase,
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
        LEFT JOIN input_val i USING (block_number, tx_id)
        LEFT JOIN output_val o USING (block_number, tx_id)
        LEFT JOIN coinbase C USING (block_number, tx_id)
)
SELECT
    *
FROM
    transactions_final
