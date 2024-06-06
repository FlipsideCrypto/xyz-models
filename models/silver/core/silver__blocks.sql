{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__blocks_tx') }}

SELECT
    VALUE,
    DATA :block_height :: INT AS block_number,
    DATA :block_hash :: STRING AS block_hash,
    DATA :block_timestamp :: bigint AS block_timestamp_num,
    TO_TIMESTAMP(
        block_timestamp_num :: STRING
    ) AS block_timestamp,
    DATA :first_version :: bigint AS first_version,
    DATA :last_version :: bigint AS last_version,
    ARRAY_SIZE(
        DATA :transactions
    ) AS tx_count_from_transactions_array,
    last_version - first_version + 1 AS tx_count_from_versions,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks_tx') }}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(
                DATEADD(
                    'minute',
                    -5,
                    modified_timestamp
                )
            )
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__blocks_tx_FR') }}
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_number
ORDER BY
    inserted_timestamp DESC)) = 1
