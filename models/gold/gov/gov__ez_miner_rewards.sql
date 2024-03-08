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
    coinbase_decoded,
    total_reward,
    block_reward,
    fees,
    COALESCE(
        block_miner_rewards_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number']
        ) }}
    ) AS ez_miner_rewards_id,
    inserted_timestamp,
    modified_timestamp
FROM
    blocks
