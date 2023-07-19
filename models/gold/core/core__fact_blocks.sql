{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH blocks AS (

    SELECT
        block_timestamp,
        block_number,
        block_hash,
        median_time,
        tx_count,
        next_block_hash,
        previous_block_hash,
        bits,
        chainwork,
        difficulty,
        merkle_root,
        nonce,
        SIZE,
        stripped_size,
        version,
        weight,
        error,
        tx AS txs
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    *
FROM
    blocks
