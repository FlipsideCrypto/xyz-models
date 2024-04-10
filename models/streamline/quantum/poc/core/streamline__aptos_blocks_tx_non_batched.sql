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

    SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height
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
    LIMIT 200
)
SELECT
    DATE_PART('EPOCH', CURRENT_TIMESTAMP())::INTEGER AS created_at,
    ROUND(block_height,-3) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET', -- request method
        calls, -- request url
        {}, -- request headers
        {}, -- request body
        'vault/dev/aptos/node/mainnet' -- aws secret manager entry, contents of which is used 
                                       -- to string interpolate the url
    ) AS request
FROM
    node_calls
