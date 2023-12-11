{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'TRANSACTIONS' }}},
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
        version,
        COALESCE(
            transactions_final_id,
            {{ dbt_utils.generate_surrogate_key(
                ['tx_id']
            ) }}
        ) AS fact_transactions_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__transactions_final') }}
)
SELECT
    *
FROM
    txs
