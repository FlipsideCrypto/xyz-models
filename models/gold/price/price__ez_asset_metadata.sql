{{ config(
    materialized = 'view'
) }}

SELECT
    token_address_lower AS token_address,
    asset_id AS id,
    asset_id,
    symbol,
    NAME,
    decimals,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    asset_metadata_priority_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__asset_metadata_priority') }}
