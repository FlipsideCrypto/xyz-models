{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'exploded_key','[\"result\", \"tx\"]', 'producer_batch_size',100, 'producer_limit_size', 1000000, 'worker_batch_size',10))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    enabled = False
) }}

WITH last_3_days AS (
    {% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number

    {% else %}

    SELECT
        MAX(block_number) - 500 AS block_number --aprox 3 days
    FROM
        {{ ref("bronze__streamline_blocks_hash") }}
    {% endif %}),
    tbl AS (
        SELECT
            block_number,
            data:result::STRING AS block_hash
        FROM
            {{ ref("bronze__streamline_FR_blocks_hash") }}
        WHERE
            (
                block_number >= (
                    SELECT
                        block_number
                    FROM
                        last_3_days
                )
            )
            AND block_number <= 213840
            AND block_number IS NOT NULL
            AND block_number NOT IN (
                SELECT
                    block_number
                FROM
                    {{ ref("streamline__complete_transactions") }}
                WHERE
                    block_number >= (
                        SELECT
                            block_number
                        FROM
                            last_3_days
                    )
                    AND block_number IS NOT NULL
                    AND block_number <= 213840

            )
    )
SELECT
    block_number,
    'getblock' AS method,
    CONCAT(
        block_hash,
        '_-_',
        '2'
    ) AS params
FROM
    tbl
