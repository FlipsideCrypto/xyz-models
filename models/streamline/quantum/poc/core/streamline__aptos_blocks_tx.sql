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

-- WITH ephemeral_model AS (
--     SELECT *
--     FROM {{ ref('streamline__aptos_blocks_tx_ephemeral') }}
-- )
-- SELECT * FROM ephemeral_model

SELECT *
FROM {{ ref('streamline__aptos_blocks_tx_ephemeral') }}


-- WITH node_calls AS (
--     -- Generate the REST requests to the APTOS node
--     -- based on the blocks that have not been fetched yet
--     SELECT
--         '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
--         block_height
--     FROM
--         (
--             SELECT
--                 block_number AS block_height
--             FROM
--                 {{ ref('streamline__aptos_blocks') }}
--             EXCEPT
--             SELECT
--                 block_number
--             FROM
--                 aptos.streamline.complete_blocks_tx
--         )
--     ORDER BY block_height DESC
-- )
-- SELECT
--     -- Push work to streamline lambdas to fetch the data from APTOS nodes
--     -- NOTE: The `fsc-quantum-state` request header is only being  set since 
--     -- this POC is only invoked via the make directive that makes the user 
--     -- (or observer from a quantum models perspective) the one defined in your local
--     -- ~/.db/profiles.yml file. This will let the livequery backend know to run in batch "push"
--     -- streamline mode. In non POC models streamline mode will automagically be enabled based on the user
--     -- invoking the model (i.e. aws_lambda_* user via the GHA DBT profile)
--     DATE_PART('EPOCH', CURRENT_TIMESTAMP())::INTEGER AS created_at,
--     ROUND(block_height,-3) AS partition_key,
--     {{ target.database }}.live.udf_api(
--         'GET', 
--         calls, 
--         {'fsc-quantum-state':'streamline'}, 
--         {}, -- request body 
--         'vault/dev/aptos/node/mainnet'
--     ) AS request
-- FROM
--     node_calls

