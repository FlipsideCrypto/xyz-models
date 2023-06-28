-- depends_on: {{ ref('bronze__streamline_blocks_hash') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    block_number,
    data:result::STRING AS block_hash,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks_hash') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks_hash') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
