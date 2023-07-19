{{ config(
    materialized = 'view',
    tags = ['core', 'ez'],
    enabled = False
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__block_miner_rewards') }}
)
SELECT
    * exclude (
        coinbase,
        _partition_by_block_id,
        _inserted_timestamp
    )
FROM
    blocks
