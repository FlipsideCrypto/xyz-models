{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH inputs AS (

    SELECT
        * exclude (
            input_data,
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
