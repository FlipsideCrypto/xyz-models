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
        input_value_sats,
        outputs,
        output_count,
        output_value,
        output_value_sats,
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
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
    FROM
        {{ ref('silver__transactions_final') }}
)
SELECT
    *
FROM
    txs
