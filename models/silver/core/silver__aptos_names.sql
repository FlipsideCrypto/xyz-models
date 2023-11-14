{{ config(
    materialized = 'incremental',
    unique_key = "aptos_names_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['noncore','full_test']
) }}

SELECT
    domain,
    domain_with_suffix,
    expiration_timestamp,
    is_active,
    is_primary,
    last_transaction_version,
    owner_address,
    registered_address,
    subdomain,
    token_name,
    token_standard,
    {{ dbt_utils.generate_surrogate_key(
        ['token_name']
    ) }} AS aptos_names_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze_api__aptoslabs_aptos_names'
    ) }}

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

qualify(ROW_NUMBER() over(PARTITION BY token_name
ORDER BY
    last_transaction_version DESC, _inserted_timestamp DESC)) = 1
