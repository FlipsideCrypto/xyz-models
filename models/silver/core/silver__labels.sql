{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    TO_TIMESTAMP_NTZ(insert_date) AS _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'crosschain',
        'dim_labels'
    ) }}
WHERE
    blockchain = 'aptos'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY address
ORDER BY
    _inserted_timestamp DESC) = 1)
