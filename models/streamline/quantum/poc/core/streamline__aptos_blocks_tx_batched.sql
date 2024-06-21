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

-- 1. **node_calls CTE**: This CTE generates a list of `URLs` for `Aptos Node API` calls and assigns a batch number to each URL.

--    It starts by selecting block numbers from the `streamline__aptos_blocks` table that are not already present in the `aptos.streamline.complete_blocks_tx` table. For each of these block numbers, it constructs a `URL` for an `Aptos Node API` call to get information about the block. 

--    It also assigns a batch number to each `URL`, with up to 10 `URLs` per batch. This is done using the `CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0)` expression, which assigns a row number to each URL (ordered by block height in descending order), divides it by 10, and rounds up to the nearest integer. This effectively groups every 10 URLs into a batch.

--    For example, if the `streamline__aptos_blocks` table contains block numbers `1` to `1000`, and the `aptos.streamline.complete_blocks_tx` table contains block numbers `1` to `900`, the `node_calls` CTE would generate URLs for block numbers `901` to `1000`, and assign a batch number to each `URL`, with batch numbers `1` to `10` for URLs `901` to `910`, batch numbers `11` to `20` for URLs `911` to `920`, and so on.

-- 2. **batches CTE**: This CTE groups the URLs into batches and calculates a partition key for each batch.

--    It does this by aggregating the `URLs` for each batch into an array using the `ARRAY_AGG(calls) AS calls` expression. It also calculates the average block number for each batch and rounds it to the nearest multiple of `1000` using the `ROUND(block_height,-3) AS partition_key` expression.

--    For example, if batch `1` contains URLs for block numbers `901` to `910`, the `batches` CTE would aggregate these URLs into an array and calculate the partition key as `ROUND(905.5, -3) = 1000`. Similarly, if batch 2 contains URLs for block numbers 911 to 920, the `batches` CTE would aggregate these URLs into an array and calculate the partition key as `ROUND(915.5, -3) = 1000`, and so on.

-- These two `CTEs` are preparing the data for making batched `Livequery API` calls. The `node_calls` CTE generates the URLs for the API calls and assigns a batch number to each URL, and the `batches` CTE groups the URLs into batches and calculates a partition key for each batch.

-- The final SELECT is designed to make API calls for each URL in the batches and store the results in a logical table (CTE resultset). It does this by unnesting the array of `URLs` for each batch and making an `API` call via `live.udf_api`.

-- The `TABLE(FLATTEN(input => calls)) AS t(VALUE)` part of the query is where the unnesting happens. The `FLATTEN` function is used to transform a semi-structured data type (like an array) into a relational representation. In this case, it's being used to unnest the calls array from the batches CTE.

-- The `input => calls` argument tells `FLATTEN` to unnest the calls array. The `FLATTEN` function returns a table with a single column named `VALUE`. This column contains the unnested elements of the array. The AS `t(VALUE)` part of the query renames this column to VALUE.

-- For example, if the batches CTE produces the following table:

-- | batch_number | calls                  | partition_key |
-- |--------------|------------------------|---------------|
-- | 1            | ['url1', 'url2', 'url3'] | 1000        |
-- | 2            | ['url4', 'url5', 'url6'] | 2000        |

-- The `TABLE(FLATTEN(input => calls))` AS `t(VALUE)` part of the query would unnest the calls array and produce the following table:

-- | VALUE | partition_key |
-- |-------|---------------|
-- | url1  | 1000          |
-- | url2  | 1000          |
-- | url3  | 1000          |
-- | url4  | 2000          |
-- | url5  | 2000          |
-- | url6  | 2000          |


-- Lets assume `streamline__aptos_blocks` produces the following blocks that need to be ingested:

-- 1000
-- 2000
-- 3000
-- 4000
-- 5000

-- And aptos.streamline.complete_blocks_tx has the following blocks ingested:

-- 1000
-- 2000

-- The EXCEPT clause in the subquery will return the block_number values that are in `streamline__aptos_block`s but not in `aptos.streamline.complete_blocks_tx`. In this case, it will return `3000`, `4000`, and `5000`.

-- The `node_calls` CTE will then generate a REST request for each of these block_number values. The calls column will contain the REST request URL, the block_height column will contain the block_number value, and the batch_number column will contain the batch number for each block_number value. The batch number is calculated by dividing the row number (when ordered by block_height in descending order) by 10 and rounding up to the nearest integer.

-- Here's what the node_calls CTE will look like:

-- calls	block_height	batch_number

-- '{service}/{Authentication}/v1/blocks/by_height/5000?with_transactions=true'	5000	1
-- '{service}/{Authentication}/v1/blocks/by_height/4000?with_transactions=true'	4000	1
-- '{service}/{Authentication}/v1/blocks/by_height/3000?with_transactions=true'	3000	1

-- The `batches CTE`' will then group the URLs by batch_number and calculate the partition_key for each batch. The partition_key is the average block_height in each batch, rounded to the nearest thousand.

-- Here's what the batches CTE will look like:

-- batch_number	calls	partition_key
-- 1	['{service}/{Authentication}/v1/blocks/by_height/5000?with_transactions=true', '{service}/{Authentication}/v1/blocks/by_height/4000?with_transactions=true', '{service}/{Authentication}/v1/blocks/by_height/3000?with_transactions=true']	4000

-- The final SELECT statement will then add a created_at column with the current timestamp and a request column with the result of the udf_api function call for each calls value in each batch.

-- Here's what the final result will look like:

-- created_at	partition_key	request
-- 2022-01-01 00:00:00	4000	{result of udf_api function call for block 5000}
-- 2022-01-01 00:00:00	4000	{result of udf_api function call for block 4000}
-- 2022-01-01 00:00:00	4000	{result of udf_api function call for block 3000}



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
