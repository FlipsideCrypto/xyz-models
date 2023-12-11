{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BITCOIN, MINING' }} },
    tags = ['core', 'ez']
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__block_miner_rewards') }}
)
SELECT
    block_timestamp,
    block_number,
    block_hash,
    total_reward,
    block_reward,
    fees,
    COALESCE(
        block_miner_rewards_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }}
    ) AS ez_miner_rewards_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
FROM
    blocks
