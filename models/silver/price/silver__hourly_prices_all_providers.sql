{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour', 'provider'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    HOUR,
    COALESCE(LOWER(b.token_address), A.token_address) AS token_address,
    provider,
    price,
    is_imputed,
    {{ dbt_utils.generate_surrogate_key(
        ['a.token_address','hour','provider']
    ) }} AS hourly_prices_all_providers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__hourly_prices_all_providers') }} A
    LEFT JOIN {{ ref('bronze__manual_token_price_metadata') }}
    b
    ON LOWER(
        A.token_address
    ) = LOWER(
        b.token_address_raw
    )
WHERE
    (
        (
            A.blockchain = 'aptos'
            AND A.token_address LIKE '%:%'
        )
        OR (
            A.blockchain = 'ethereum'
            AND A.token_address IN (
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xd31a59c85ae9d8edefec411d448f90841571b89c',
                '0x4e15361fd6b4bb609fa63c81a2be19d873717870',
                '0x7c9f4c87d911613fe9ca58b579f737911aad2d43'
            )
        )
    )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
