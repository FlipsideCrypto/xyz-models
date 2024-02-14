{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'address',
    tags = ["core", "scheduled_non_core"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;"
) }}

SELECT
    TO_TIMESTAMP_NTZ(system_created_at) AS system_created_at,
    TO_TIMESTAMP_NTZ(insert_date) AS insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    source,
    modified_timestamp AS _modified_timestamp,
    labels_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__labels') }}

{% if is_incremental() %}
WHERE
    _modified_timestamp >= (
        SELECT
            MAX(
                _modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY lower(address)
    ORDER BY
        _modified_timestamp DESC
) = 1
