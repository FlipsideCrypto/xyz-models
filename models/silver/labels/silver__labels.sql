{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'labels_id',
    tags = ["core", "scheduled_non_core"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;"
) }}

SELECT
    system_created_at :: TIMESTAMP_NTZ AS system_created_at,
    insert_date :: TIMESTAMP_NTZ AS insert_date,
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
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS labels_id,
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
