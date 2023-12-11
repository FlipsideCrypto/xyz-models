{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ["block_number", "block_timestamp::DATE"],
    tags = ["core", "ez", "scheduled_non_core" ],
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
transactions AS (
    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    block_number IN (
        SELECT
            DISTINCT block_number
        FROM
            blocks
    )
{% endif %}
),
tx_value AS (
    SELECT
        block_number,
        SUM(COALESCE(fee, 0)) AS fees
    FROM
        transactions
    GROUP BY
        1
),
coinbase AS (
    SELECT
        block_number,
        coinbase,
        output_value AS coinbase_value
    FROM
        transactions
    WHERE
        is_coinbase
),
blocks_final AS (
    SELECT
        block_timestamp,
        b.block_number,
        b.block_hash,
        C.coinbase_value AS total_reward,
        C.coinbase_value - v.fees AS block_reward,
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
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS block_miner_rewards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks_final
