{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"1000000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"5000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH blocks AS (

    SELECT
        A.block_number,
        tx_count_from_versions -100 AS tx_count,
        first_version + 100 version_start
    FROM
        {{ ref('silver__blocks') }} A
    WHERE
        tx_count_from_versions > 100
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
                ) AS tx_version,
                multiplier
            FROM
                blocks_with_page_numbers A
                LEFT JOIN {{ ref('streamline__transactions_complete') }}
                b
                ON A.block_number = b.block_number
                AND multiplier = b.multiplier_no
            WHERE
                b.block_number IS NULL
        )
    SELECT
        tx_version,
        ROUND(
            tx_version,
            -4
        ) :: INT AS partition_key,
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/v1/transactions?start=' || tx_version || '&limit=100',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            PARSE_JSON('{}'),
            'Vault/prod/m1/devnet'
        ) AS request,
        block_number,
        multiplier
    FROM
        WORK
    ORDER BY
        block_number
