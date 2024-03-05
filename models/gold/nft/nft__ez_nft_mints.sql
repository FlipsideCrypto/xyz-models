{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH base AS (

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
        total_price_raw / pow(
            10,
            b.decimals
        ) AS total_price,
        total_price * C.price AS total_price_usd,
        currency_address,
        fact_nft_mints_id AS ez_nft_mints_id,
        GREATEST(
            A.inserted_timestamp,
            b.inserted_timestamp
        ) AS inserted_timestamp,
        GREATEST(
            A.modified_timestamp,
            b.modified_timestamp
        ) AS modified_timestamp,
        b.decimals,
        b.symbol,
        C.price
    FROM
        {{ ref('nft__fact_nft_mints') }} A
        LEFT JOIN {{ ref('core__dim_tokens') }}
        b
        ON A.currency_address = b.token_address
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }} C
        ON LOWER(
            A.currency_address
        ) = C.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = C.hour
)
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
    total_price,
    total_price_usd,
    currency_address,
    ez_nft_mints_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
