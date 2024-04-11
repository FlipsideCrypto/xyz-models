{{ config (
    materialized = "ephemeral",
    tags = ['quantum_poc_ephemeral']
) }}

WITH node_calls AS (
    -- Generate the REST requests to the APTOS node
    -- based on the blocks that have not been fetched yet
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
)
SELECT
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP(3) AS _inserted_timestamp,
    ROUND(block_height,-3) AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET', 
        calls, 
        {}, -- request headers 
        {}, -- request body 
        'vault/dev/aptos/node/mainnet'
    ) AS request
FROM
    node_calls