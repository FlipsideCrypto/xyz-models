{{ config(
    materialized = 'view'
) }}

WITH outputs AS (

    SELECT
        * exclude (
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
