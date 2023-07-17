{{ config(
    materialized = 'view'
) }}

WITH inputs AS (

    SELECT
        * exclude (
            _partition_by_block_id,
            _inserted_timestamp
        )
    FROM
        {{ ref('silver__inputs') }}
)
SELECT
    *
FROM
    inputs
