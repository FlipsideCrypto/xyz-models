{{ config (
    materialized = 'view'
) }}

SELECT
    block_number, 
    data:result::STRING AS block_hash
FROM
    {{ source(
        'bronze_streamline',
        'blocks_hash'
    ) }}
WHERE block_hash IS NOT NULL