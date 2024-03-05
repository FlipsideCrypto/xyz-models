{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    nft_from_address,
    nft_to_address,
    nft_address,
    token_version,
    project_name,
    tokenid,
    nft_count,
    total_price_raw,
    currency_address,
    nft_mints_combined_id AS fact_nft_mints_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_mints_combined') }}
