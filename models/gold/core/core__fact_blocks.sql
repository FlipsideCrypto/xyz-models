{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BLOCKS' }}},
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
        tx AS txs,
        COALESCE(
            blocks_id,
            {{ dbt_utils.generate_surrogate_key(
                ['block_number']
            ) }}
        ) AS fact_blocks_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    *
FROM
    blocks
