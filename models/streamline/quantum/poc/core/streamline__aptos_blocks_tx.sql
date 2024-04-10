{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
         params = {
            "external_table": "quantum_poc/aptos_blocks_tx",
            "sql_limit": "10",
            "producer_batch_size": "10",
            "worker_batch_size": "10",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['quantum_poc']
) }}

WITH node_calls AS (
    -- generate a list of URLs for API calls and assign a batch number to each
    SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height,
        CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0) AS batch_number
    FROM
        (
            SELECT
                block_number AS block_height
            FROM
                {{ ref('streamline__aptos_blocks') }}
            EXCEPT
            SELECT
                block_number
            FROM
                aptos.streamline.complete_blocks_tx
        )
    ORDER BY block_height DESC
    LIMIT 1000
),
batches AS (
    -- group URLs by batch number and calculate the partition key
    SELECT
        batch_number,
        ARRAY_AGG(calls) AS calls,
        ROUND(AVG(block_height),-3) AS partition_key
    FROM
        node_calls
    GROUP BY
        batch_number
)
SELECT
    DATE_PART('EPOCH', CURRENT_TIMESTAMP())::INTEGER AS created_at,
    partition_key,
    {{ target.database }}.live.udf_api(
        'GET', -- request method
        t.VALUE, -- request url
        {}, -- request headers
        {}, -- request body
        'vault/dev/aptos/node/mainnet'
    ) AS request
FROM
    batches,
    TABLE(FLATTEN(input => calls)) AS t(VALUE)