-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(_partition_by_block_id, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    block_number,
    _partition_by_block_id,
    A._inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_batch') }}
{% else %}
    {{ ref('bronze__streamline_FR_transaction_batch') }}
{% endif %}

A
JOIN {{ ref('silver__blocks') }}
b
ON DATA [0] :version :: INT BETWEEN b.first_version
AND b.last_version

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            A._inserted_timestamp DESC)) = 1
