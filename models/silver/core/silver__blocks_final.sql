{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ["block_number", "block_timestamp::DATE"],
    tags = ["core"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}

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
transactions_final AS (
    SELECT
        *
    FROM
        {{ ref('silver__transactions_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    OR (
        _partition_by_block_id IN (
            SELECT
                DISTINCT _partition_by_block_id
            FROM
                blocks
        )
        AND block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                blocks
        )
    )
{% endif %}
),
tx_value AS (
    SELECT
        block_number,
        SUM(input_value) AS input_value,
        SUM(output_value) AS output_value,
        SUM(fee) AS fee
    FROM
        transactions_final
    GROUP BY
        1
),
blocks_final AS (
    SELECT
        b.block_number,
        bits,
        chainwork,
        difficulty,
        block_hash,
        median_time,
        merkle_root,
        tx_count,
        v.input_value,
        v.output_value,
        v.fee,
        next_block_hash,
        nonce,
        previous_block_hash,
        stripped_size,
        SIZE,
        block_timestamp,
        tx,
        version,
        weight,
        error,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        blocks b
        LEFT JOIN tx_value v USING (block_number)
)
SELECT
    *
FROM
    blocks_final
