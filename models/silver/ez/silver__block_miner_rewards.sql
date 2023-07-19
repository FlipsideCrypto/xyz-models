{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ["block_number", "block_timestamp::DATE"],
    tags = ["core", "ez"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    enabled = False
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
        SUM(fee) AS fees
    FROM
        transactions_final
    GROUP BY
        1
),
-- TODO - i can pull output value out in coinbase transactions and avoid the join, here
coinbase AS (
    SELECT
        block_number,
        coinbase,
        output_value
    FROM
        transactions_final
    WHERE
        is_coinbase
),
blocks_final AS (
    SELECT
        block_timestamp,
        b.block_number,
        b.block_hash,
        C.output_value AS total_reward,
        C.output_value - v.fees AS block_reward,
        v.fees,
        C.coinbase,
        b._partition_by_block_id,
        b._inserted_timestamp
    FROM
        blocks b
        LEFT JOIN tx_value v USING (block_number)
        LEFT JOIN coinbase C USING (block_number)
)
SELECT
    *
FROM
    blocks_final
