{{ config(
    materialized = 'incremental',
    unique_key = ['token_address','symbol','provider', 'asset_id', 'name'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    asset_id,
    token_address,
    LOWER(token_address) AS token_address_lower,
    COALESCE(
        C.symbol,
        p.symbol
    ) AS symbol,
    p.name,
    decimals,
    platform,
    platform_id,
    p.provider,
    source,
    complete_provider_asset_metadata_id AS asset_metadata_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__complete_provider_asset_metadata') }}
    p
    LEFT JOIN {{ ref('silver__coin_info') }} C
    ON LOWER(
        C.coin_type
    ) = LOWER(p.token_address)

{% if is_incremental() %}
WHERE
    p.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, asset_id, COALESCE(C.symbol, p.symbol), provider
ORDER BY
    p._inserted_timestamp DESC)) = 1
