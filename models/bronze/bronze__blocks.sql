{{ config (
    materialized = 'view'
) }}

SELECT
    block_height,
    DATA,
    _inserted_timestamp
FROM
    {{ source(
        'bronze',
        'sample_blocks'
    ) }}
WHERE
    DATA: error IS NULL 

qualify(ROW_NUMBER() over (PARTITION BY block_height
ORDER BY
    _inserted_timestamp DESC)) = 1
