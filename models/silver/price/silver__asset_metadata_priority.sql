{{ config(
    materialized = 'incremental',
    unique_key = ['token_address'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    p.token_address,
    LOWER(
        p.token_address
    ) AS token_address_lower,
    p.asset_id,
    COALESCE(
        C.symbol,
        p.symbol
    ) AS symbol,
    C.name,
    C.decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    CASE
        WHEN p.provider = 'coingecko' THEN 1
        WHEN p.provider = 'coinmarketcap' THEN 2
    END AS priority,
    complete_token_asset_metadata_id AS asset_metadata_priority_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__complete_token_asset_metadata') }}
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

qualify(ROW_NUMBER() over (PARTITION BY token_address
ORDER BY
    priority ASC)) = 1
