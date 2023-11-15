-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(tx_version, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    partition_key AS tx_version,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_transactions') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
