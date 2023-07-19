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
        size,
        stripped_size,
        tx as txs,
        version,
        weight,
        error
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    *
FROM
    blocks
