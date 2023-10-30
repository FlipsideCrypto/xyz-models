{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    first_version,
    last_version,
    tx_count_from_versions AS tx_count,
    blocks_id AS fact_blocks_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__blocks'
    ) }}
