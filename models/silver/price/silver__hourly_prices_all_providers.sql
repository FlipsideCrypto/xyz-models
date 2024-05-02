{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour', 'provider'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    recorded_hour AS HOUR,
    LOWER(COALESCE(b.token_address, m.token_address)) AS token_address_lower,
    COALESCE(
        b.token_address,
        m.token_address
    ) AS token_address,
    A.asset_id,
    OPEN,
    high,
    low,
    CLOSE,
    CLOSE AS price,
    NULL AS is_imputed,
    A.provider,
    A.source,
    complete_provider_prices_id AS hourly_prices_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__complete_provider_prices') }} A
    INNER JOIN (
        SELECT
            asset_id,
            token_address,
            symbol,
            modified_timestamp
        FROM
            {{ ref('bronze__complete_provider_asset_metadata') }}
            qualify(ROW_NUMBER() over (PARTITION BY asset_id
        ORDER BY
            modified_timestamp DESC) = 1)
    ) m
    ON A.asset_id = m.asset_id
    LEFT JOIN {{ ref('bronze__manual_token_price_metadata') }}
    b
    ON LOWER(
        m.token_address
    ) = LOWER(
        b.token_address_raw
    )

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        m.modified_timestamp
    ) >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY COALESCE(b.token_address, m.token_address), HOUR, provider
ORDER BY
    A.modified_timestamp DESC)) = 1
