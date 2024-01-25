{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} },
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    bridge_address,
    event_name,
    platform,
    sender,
    destination_chain_receiver,
    destination_chain,
    destination_chain_id
    token_address,
    amount_unadj,
    COALESCE (
        bridge_wormhole_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_wormhole_transfers') }}
union all 
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    bridge_address,
    event_name,
    platform,
    sender,
    destination_chain_receiver,
    destination_chain,
    destination_chain_id
    token_address,
    amount_unadj,
    COALESCE (
        bridge_layerzero_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_layerzero_transfers') }}
union all
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    bridge_address,
    event_name,
    platform,
    sender,
    destination_chain_receiver,
    destination_chain,
    destination_chain_id
    token_address,
    amount_unadj,
    COALESCE (
        bridge_mover_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_mover_transfers') }}
union all
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    bridge_address,
    event_name,
    platform,
    sender,
    destination_chain_receiver,
    destination_chain,
    destination_chain_id
    token_address,
    amount_unadj,
    COALESCE (
        bridge_celer_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_celer_transfers') }}
