{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    p.hour,
    p.token_address,
    p.price,
    p.is_imputed,
    COALESCE(
        C.symbol,
        m.symbol
    ) AS symbol,
    C.decimals AS decimals,
    {{ dbt_utils.generate_surrogate_key(
        ['p.token_address','hour']
    ) }} AS hourly_prices_priority_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__hourly_prices_priority') }}
    p
    LEFT JOIN {{ ref('silver__asset_metadata_priority') }}
    m
    ON LOWER(
        p.token_address
    ) = LOWER(
        m.token_address
    )
    LEFT JOIN {{ ref('silver__coin_info') }} C
    ON C.coin_type = p.token_address

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
