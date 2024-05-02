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
    provider,
    inserted_timestamp,
    modified_timestamp,
    asset_metadata_all_providers_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__asset_metadata_all_providers') }}
