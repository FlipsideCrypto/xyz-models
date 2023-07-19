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
        index,
        tx_hash,
        HEX,
        FEE,
        IS_COINBASE,
        COINBASE,
        INPUTS,
        INPUT_COUNT,
        INPUT_VALUE,
        OUTPUTS,
        OUTPUT_COUNT,
        OUTPUT_VALUE,
        SIZE,
        VIRTUAL_SIZE,
        WEIGHT,
        LOCK_TIME,
        VERSION
    FROM
        {{ ref('silver__transactions_final') }}
)
SELECT
    *
FROM
    txs
