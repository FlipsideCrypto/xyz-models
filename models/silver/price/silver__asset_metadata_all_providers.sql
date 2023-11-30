{{ config(
    materialized = 'incremental',
    unique_key = ['asset_metadata_all_providers_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    token_address,
    id,
    COALESCE(
        C.symbol,
        p.symbol
    ) AS symbol,
    NAME,
    decimals,
    provider,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address','provider','COALESCE( C.symbol, p.symbol ) ','id']
    ) }} AS asset_metadata_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__asset_metadata_all_providers') }}
    p
    LEFT JOIN {{ ref('silver__coin_info') }} C
    ON C.coin_type = p.token_address

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, id, COALESCE(C.symbol, p.symbol), provider
ORDER BY
    p._inserted_timestamp DESC)) = 1
