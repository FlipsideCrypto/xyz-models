{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'tx_receipts', 'producer_batch_size',10000, 'producer_limit_size', 10000000, 'worker_batch_size',200))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number
    {% else %}
    SELECT
        MAX(block_number) - 10000 AS block_number --aprox 3 days
    FROM
        {{ ref("streamline__complete_blocks") }}
    {% endif %}),
    flattened_tbl AS (
        SELECT
            block_number,
            VALUE :: STRING AS tx_hash
        FROM
            {{ ref("streamline__complete_blocks") }},
            LATERAL FLATTEN(
                input => transactions
            )
        WHERE transactions IS NOT NULL
    ),
    tbl AS (
        SELECT
            block_number,
            tx_hash
        FROM
            flattened_tbl
        WHERE
            (
                block_number >= (
                    SELECT
                        block_number
                    FROM
                        last_3_days
                )
            )
            AND block_number IS NOT NULL
            AND tx_hash IS NOT NULL
            AND tx_hash NOT IN (
                SELECT
                    tx_hash
                FROM
                    {{ ref("streamline__complete_tx_receipts") }}
            )
    )
SELECT
    block_number,
    'eth_getTransactionReceipt' AS method,
    tx_hash AS params
FROM
    tbl
