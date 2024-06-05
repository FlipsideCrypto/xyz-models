{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "transactions_complete_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}
-- depends_on: {{ ref('bronze__transactions') }}

SELECT
    A.value :BLOCK_NUMBER :: INT AS block_number,
    A.value :MULTIPLIER :: INT AS multiplier_no,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','multiplier_no']
    ) }} AS transactions_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
{% else %}
    {{ ref('bronze__transactions_FR') }}
{% endif %}

A
JOIN {{ ref('silver__blocks') }}
b
ON DATA [0] :version :: INT BETWEEN b.first_version
AND b.last_version

{% if is_incremental() %}
WHERE
    A.inserted_timestamp >= (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: DATE)
        FROM
            {{ this }})
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            A.inserted_timestamp DESC)) = 1
