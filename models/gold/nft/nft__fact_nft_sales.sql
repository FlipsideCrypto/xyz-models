{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    buyer_address,
    seller_address,
    nft_address,
    token_version,
    platform_address,
    project_name,
    tokenid,
    platform_name,
    platform_exchange_version,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    aggregator_name,
    currency_address,
    nft_sales_combined_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_combined') }}
