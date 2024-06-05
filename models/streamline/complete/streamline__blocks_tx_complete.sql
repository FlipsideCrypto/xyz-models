{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}
-- depends_on: {{ ref('bronze__blocks_tx') }}

SELECT
    DATA :block_height :: INT AS block_number,
    ARRAY_SIZE(
        DATA :transactions
    ) AS tx_count_from_transactions_array,
    DATA :last_version :: bigint - DATA :first_version :: bigint + 1 AS tx_count_from_versions,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS blocks_tx_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks_tx') }}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__blocks_tx_FR') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    inserted_timestamp DESC)) = 1
