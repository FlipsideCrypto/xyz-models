{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH txs AS (

    SELECT
        block_timestamp,
        block_number,
        block_hash,
        tx_id,
        INDEX,
        tx_hash,
        hex,
        fee,
        is_coinbase,
        coinbase,
        inputs,
        input_count,
        input_value,
        outputs,
        output_count,
        output_value,
        SIZE,
        virtual_size,
        weight,
        lock_time,
        version
    FROM
        {{ ref('silver__transactions_final') }}
)
SELECT
    *
FROM
    txs
