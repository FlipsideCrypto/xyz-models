{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

SELECT
    block_height AS block_number,
    DATA :data :block_hash :: STRING AS block_hash,
    -- DATA :data :block_height :: INT AS block_height,
    DATA :data :block_timestamp :: STRING AS block_timestamp_num,
    TO_TIMESTAMP(block_timestamp_num) AS block_timestamp,
    DATA :data :first_version :: bigint AS first_version,
    DATA :data :last_version :: bigint AS last_version,
    ARRAY_SIZE(
        DATA :data :transactions
    ) AS tx_count_from_transactions_array,
    last_version - first_version + 1 AS tx_count_from_versions,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'aptos_bronze',
        'lq_blocks_txs'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
