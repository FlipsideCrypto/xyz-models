{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    tags = ['core']
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
    project_name
FROM
    {{ ref('bronze__labels') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(
                insert_date
            )
        FROM
            {{ this }}
    )
{% endif %}
