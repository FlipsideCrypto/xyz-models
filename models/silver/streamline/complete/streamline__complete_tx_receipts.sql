-- depends_on: {{ ref('bronze__streamline_tx_receipts') }}

{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    block_number,
    data:result:transactionHash::STRING AS tx_hash,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tx_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND tx_hash IS NOT NULL
{% else %}
    {{ ref('bronze__streamline_FR_tx_receipts') }}
    WHERE tx_hash IS NOT NULL
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
