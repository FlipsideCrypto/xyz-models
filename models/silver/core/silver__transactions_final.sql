{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ['block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = 'tx_id',
    cluster_by = ["block_number", "tx_id"],
    tags = ["core", "scheduled_core"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
inputs AS (
    SELECT
        *
    FROM
        {{ ref('silver__inputs_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
input_val AS (
    SELECT
        block_number,
        tx_id,
        SUM(VALUE) AS input_value
    FROM
        inputs
    GROUP BY
        1,
        2
),
transactions_final AS (
    SELECT
        t.block_number,
        block_hash,
        block_timestamp,
        t.tx_id,
        INDEX,
        tx_hash,
        hex,
        lock_time,
        SIZE,
        version,
        is_coinbase,
        coinbase,
        inputs,
        input_count,
        i.input_value,
        outputs,
        output_count,
        output_value,
        virtual_size,
        weight,
        IFF(
            is_coinbase,
            0,
            fee
        ) AS fee,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        transactions t
        LEFT JOIN input_val i USING (
            block_number,
            tx_id
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transactions_final
