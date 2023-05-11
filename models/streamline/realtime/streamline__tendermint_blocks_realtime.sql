{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('url_route','tendermint_rpc', 'sql_source', '{{this.identifier}}', 'external_table', 'tendermint_blocks', 'method', 'eth_getBlockByNumber', 'producer_batch_size',1000, 'producer_limit_size', 1000000, 'worker_batch_size',100, 'producer_batch_chunks_size', 1000))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number
    {% else %}
    SELECT
        MAX(block_number) - 100000 AS block_number --aprox 3 days
    FROM
        {{ ref("streamline__blocks") }}
    {% endif %}),
    tbl AS (
        SELECT
            block_number
        FROM
            {{ ref("streamline__blocks") }}
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
        EXCEPT
        SELECT
            block_number
        FROM
            {{ ref("streamline__complete_tendermint_blocks") }}
        WHERE
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
    )
SELECT
    block_number,
    'block' AS method,
    block_number :: STRING AS params
FROM
    tbl
