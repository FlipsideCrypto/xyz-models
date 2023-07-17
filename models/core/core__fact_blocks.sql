{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks_final') }}
)
SELECT
    * exclude (
        _partition_by_block_id,
        _inserted_timestamp
    )
FROM
    blocks
