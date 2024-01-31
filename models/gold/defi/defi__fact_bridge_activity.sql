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
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    bridge_wormhole_transfers_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_wormhole_transfers') }}
union all 
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    bridge_layerzero_transfers_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_layerzero_transfers') }}
union all
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    bridge_mover_transfers_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_mover_transfers') }}
union all
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    bridge_celer_transfers_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_celer_transfers') }}
