{{ config (
    materialized = 'view'
) }}

SELECT
    asset_id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_asset_metadata'
    ) }}
WHERE
    (
        (
            platform ILIKE 'aptos'
            AND token_address LIKE '%:%'
        )
        OR (
            platform = 'ethereum'
            AND token_address IN (
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xd31a59c85ae9d8edefec411d448f90841571b89c',
                '0x4e15361fd6b4bb609fa63c81a2be19d873717870',
                '0x7c9f4c87d911613fe9ca58b579f737911aad2d43'
            )
        )
    )
