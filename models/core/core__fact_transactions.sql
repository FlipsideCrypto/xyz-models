{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH txs AS (

    SELECT
        * exclude (
            _partition_by_block_id,
            _inserted_timestamp
        )
    FROM
        {{ ref('silver__transactions_final') }}
)
SELECT
    *
FROM
    txs
