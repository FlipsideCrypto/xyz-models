-- depends_on: {{ ref('bronze_evm__streamline_traces') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_core_evm_complete']
) }}

SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_evm_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_evm__streamline_traces') }}
WHERE
    inserted_timestamp >= (
        SELECT
            COALESCE(MAX(inserted_timestamp), '1970-01-01' :: TIMESTAMP) inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_evm__streamline_FR_traces') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            inserted_timestamp DESC)) = 1
