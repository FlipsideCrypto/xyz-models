{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['HOUR::DATE'],
    tags = ['core']
) }}

SELECT
    p.hour,
    LOWER(COALESCE(b.token_address, p.token_address)) AS token_address_lower,
    COALESCE(
        b.token_address,
        p.token_address
    ) AS token_address,
    p.price,
    p.is_imputed,
    p.is_deprecated,
    COALESCE(
        C.symbol,
        m.symbol
    ) AS symbol,
    C.decimals AS decimals,
    p.provider,
    p.source,
    COALESCE(
        C.name,
        m.name
    ) AS NAME,
    complete_token_prices_id AS hourly_prices_priority_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__complete_token_prices') }}
    p
    LEFT JOIN {{ ref('bronze__manual_token_price_metadata') }}
    b
    ON LOWER(
        p.token_address
    ) = LOWER(
        b.token_address_raw
    )
    LEFT JOIN {{ ref('silver__asset_metadata_priority') }}
    m
    ON LOWER(
        p.token_address
    ) = LOWER(
        m.token_address
    )
    LEFT JOIN {{ ref('silver__coin_info') }} C
    ON LOWER(
        C.coin_type
    ) = LOWER(COALESCE(b.token_address, p.token_address))
WHERE
    (
        (
            p.blockchain = 'aptos'
            AND p.token_address LIKE '%:%'
        )
        OR (
            p.blockchain = 'ethereum'
            AND p.token_address IN (
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xd31a59c85ae9d8edefec411d448f90841571b89c',
                '0x4e15361fd6b4bb609fa63c81a2be19d873717870',
                '0x7c9f4c87d911613fe9ca58b579f737911aad2d43'
            )
        )
    )

{% if is_incremental() %}
AND p.modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY hour,token_address
                    ORDER BY
                    hour DESC)) = 1
