{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour', 'provider'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    HOUR,
    token_address,
    provider,
    price,
    is_imputed,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address','hour','provider']
    ) }} AS hourly_prices_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__hourly_prices_all_providers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
