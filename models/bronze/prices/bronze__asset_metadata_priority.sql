{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    token_address,
    symbol,
    provider,
    id,
    _inserted_timestamp,
    modified_timestamp,
    inserted_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'asset_metadata_priority'
    ) }}
WHERE
    blockchain = 'aptos'
    AND token_address LIKE '%:%'
