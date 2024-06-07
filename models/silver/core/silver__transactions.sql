{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    incremental_predicates = ['block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = 'tx_id',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"],
    tags = ["core", "scheduled_core"]
) }}
-- depends_on: {{ ref('silver__blocks') }}
WITH bronze_transactions AS (

    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND block_number <= (
        SELECT
            MAX(block_number)
        FROM
            {{ ref('silver__blocks') }}
    )
{% endif %}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__transaction_index') }}

{% if is_incremental() %}
WHERE
    block_number IN (
        SELECT
            DISTINCT block_number
        FROM
            bronze_transactions
    )
{% endif %}
),
FINAL AS (
    SELECT
        t.block_number,
        b.block_hash,
        b.block_timestamp,
        t.tx_id,
        b.index,
        DATA: vin [0]: coinbase IS NOT NULL AS is_coinbase,
        DATA: vin [0]: coinbase :: STRING AS coinbase,
        DATA :hash :: STRING AS tx_hash,
        DATA :hex :: STRING AS hex,
        DATA :locktime :: STRING AS lock_time,
        DATA :size :: NUMBER AS SIZE,
        DATA :version :: NUMBER AS version,
        DATA :vin :: ARRAY AS inputs,
        ARRAY_SIZE(inputs) AS input_count,
        DATA :vout :: ARRAY AS outputs,
        {{ target.database }}.silver.udf_sum_vout_values(outputs) AS output_value_agg,
        to_decimal(output_value_agg, 17, 8) AS OUTPUT_VALUE,
        (to_decimal(output_value_agg, 17, 8) * pow(10,8)) :: INTEGER as OUTPUT_VALUE_SATS,
        ARRAY_SIZE(outputs) AS output_count,
        DATA :vsize :: STRING AS virtual_size,
        DATA :weight :: STRING AS weight,
        DATA: fee :: FLOAT AS fee,
        t._partition_by_block_id,
        t._inserted_timestamp
    FROM
        bronze_transactions t
        LEFT JOIN blocks b USING (
            block_number,
            tx_id
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
