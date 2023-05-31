{{ config(
    materialized = 'view'
) }}

SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    t.tx_from,
    t.tx_succeeded,
    t.codespace,
    t.fee,
    t.gas_used,
    t.gas_wanted,
    t.msgs
FROM
    {{ ref('silver__transactions_final') }}
    t
