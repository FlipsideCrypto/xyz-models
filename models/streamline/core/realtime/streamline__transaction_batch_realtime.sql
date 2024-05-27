{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transaction_batch', 'sql_limit', {{var('sql_limit','1200000')}}, 'producer_batch_size', {{var('producer_batch_size','300000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','prod/aptos/node/mainnet'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}
-- depends_on: {{ ref('bronze__streamline_transaction_batch') }}
WITH blocks AS (

    SELECT
        A.block_number,
        tx_count_from_versions -100 AS tx_count,
        first_version + 100 version_start
    FROM
        {{ ref('silver__blocks') }} A
    WHERE
        tx_count_from_versions > 100
        AND block_number >= 183074459
),
numbers AS (
    -- Recursive CTE to generate numbers. We'll use the maximum txcount value to limit our recursion.
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(tx_count) / 100.0)
            FROM
                blocks)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number :: INT AS block_number,
                n.n - 1 AS multiplier,
                version_start,
                tx_count
            FROM
                blocks tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.tx_count % 100 = 0 THEN tt.tx_count / 100
                    ELSE FLOOR(
                        tt.tx_count / 100
                    ) + 1
                END
        ),
        WORK AS (
            SELECT
                A.block_number,
                version_start +(
                    100 * multiplier
                ) AS tx_version
            FROM
                blocks_with_page_numbers A
                LEFT JOIN {{ ref('streamline__complete_transactions') }}
                b
                ON A.block_number = b.block_number
            WHERE
                b.block_number IS NULL
        ),
        calls AS (
            SELECT
                '{service}/{Authentication}/v1/transactions?start=' || tx_version || '&limit=100' calls,
                block_number
            FROM
                WORK
        )
    SELECT
        ARRAY_CONSTRUCT(
            ROUND(
                block_number,
                -3
            ) :: INT,
            ARRAY_CONSTRUCT(
                'GET',
                calls,
                PARSE_JSON('{}'),
                PARSE_JSON('{}'),
                ''
            )
        ) AS request
    FROM
        calls
    ORDER BY
        block_number
