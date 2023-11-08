{{ config(
    materialized = 'incremental',
    unique_key = "coin_type",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['core']
) }}

SELECT
    coin_type,
    coin_type_hash,
    creator_address,
    decimals,
    NAME,
    symbol,
    transaction_created_timestamp,
    transaction_version_created,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'bronze_api__aptoslabs_coin_info'
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

qualify(ROW_NUMBER() over(PARTITION BY coin_type
ORDER BY
    _inserted_timestamp DESC)) = 1
