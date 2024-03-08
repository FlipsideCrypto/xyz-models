{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = False,
    tags = ['observability']
) }}

WITH summary_stats AS (

    SELECT
        MIN(block_number) AS min_block,
        MAX(block_number) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, SYSDATE())

{% if is_incremental() %}
AND (
    block_number >= (
        SELECT
            MIN(block_number)
        FROM
            (
                SELECT
                    MIN(block_number) AS block_number
                FROM
                    {{ ref('silver__blocks') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, SYSDATE())
                    AND DATEADD('hour', -95, SYSDATE())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS block_number
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR block_number >= 0
    {% endif %}
)
{% endif %}
),
block_range AS (
    SELECT
        _id AS block_number
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        block_number BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
),
txs_per_block_actual AS (
    SELECT
        block_number,
        COUNT(
            DISTINCT tx_id
        ) AS txs
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_number IN (
            SELECT
                block_number
            FROM
                block_range
        )
    GROUP BY
        1
),
txs_per_block_expected AS (
    SELECT
        block_number,
        tx_count AS txs
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_number IN (
            SELECT
                block_number
            FROM
                block_range
        )
),
comparison AS (
    SELECT
        id.block_number,
        A.txs AS actual_tx_count,
        e.txs AS expected_tx_count
    FROM
        block_range id
        LEFT JOIN txs_per_block_actual A USING (block_number)
        LEFT JOIN txs_per_block_expected e USING (block_number)
),
impacted_blocks AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(block_number) within GROUP (
            ORDER BY
                block_number
        ) AS blocks_impacted_array
    FROM
        comparison
    WHERE
        actual_tx_count != expected_tx_count
),
FINAL AS (
    SELECT
        'transactions' AS test_name,
        min_block,
        max_block,
        min_block_timestamp,
        max_block_timestamp,
        blocks_tested,
        blocks_impacted_count,
        blocks_impacted_array,
        SYSDATE() AS test_timestamp
    FROM
        summary_stats
        JOIN impacted_blocks
)
SELECT
    *
FROM
    FINAL
