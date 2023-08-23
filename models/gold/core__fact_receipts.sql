{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['core']
) }}

SELECT
    block_number,
    block_hash,
    cumulative_gas_used,
    from_address,
    to_address,
    gas_used,
    logs,
    logs_bloom,
    status,
    tx_success,
    tx_status,
    tx_hash,
    position,
    type,
    near_receipt_hash,
    near_transaction_hash
FROM
    {{ target.database }}.silver.receipts_copy
