{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'blocks_tx', 'sql_limit', {{var('sql_limit','1200000')}}, 'producer_batch_size', {{var('producer_batch_size','300000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','prod/aptos/node/mainnet'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH gen AS (

    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) AS block_height
    FROM
        TABLE(GENERATOR(rowcount => 110000000))
),
blocks AS (
    SELECT
        block_height
    FROM
        gen
    ORDER BY
        1 DESC
),
calls AS (
    SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height
    FROM
        (
            SELECT
                block_height
            FROM
                blocks
            EXCEPT
            SELECT
                block_number
            FROM
                {{ ref('streamline__blocks_tx_complete') }}
        )
)
SELECT
    ARRAY_CONSTRUCT(
        block_height,
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
    block_height DESC
