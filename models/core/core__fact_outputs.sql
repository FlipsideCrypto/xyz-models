{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH outputs AS (

    SELECT
        * exclude (
            output_data,
            _partition_by_block_id,
            _inserted_timestamp
        )
    FROM
        {{ ref('silver__outputs') }}
)
SELECT
    *
FROM
    outputs
