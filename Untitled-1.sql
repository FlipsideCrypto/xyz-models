
SET LIVEQUERY_CONTEXT = '{"userId":"aws_lambda_datascience_api"}';
SET LIVEQUERY_CONTEXT = '{"userId":"shah"}';

SELECT * FROM DATASCIENCE_DEV.STREAMLINE.APTOS_BLOCKS_TX;  

SELECT * FROM DATASCIENCE_DEV.SILVER.BLOCKS ;

drop table datascience_dev.silver.blocks;

select * from streamline.datascience_dev.quantum_poc_aptos_blocks_tx;



use datascience_dev._live;

USE DATABASE datascience_dev;
USE SCHEMA live;
show USER functions;


SELECT
    DATE_PART('EPOCH', CURRENT_TIMESTAMP())::INTEGER AS created_at,
    datascience_dev.live.udf_api(
        'GET', -- request method
        '{service}/{Authentication}/v1/blocks/by_height/' || '167503821' || '?with_transactions=true', -- request url
        {}, -- request headers
        {}, -- request body
        'vault/dev/aptos/node/mainnet'
    ) AS request;

WITH node_calls AS (

    SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height
    FROM
        (
            SELECT column1 AS block_height
            FROM VALUES
                (167503821),
                (167503822),
                (167503823),
                (167503824)
        )
),
lq_calls as (

    SELECT
        DATE_PART('EPOCH', CURRENT_TIMESTAMP())::INTEGER AS created_at,
        ROUND(block_height,-3) AS partition_key,
        datascience_dev.live.udf_api(
            'GET', 
            calls, 
            {'fsc-quantum-state':'streaamline'}, -- note only being  set since the POC is only invoked via the make directive
                                                -- in non POC models streamline mode will be enable based on the user
                                                -- invoking the model (i.e. aws_lambda_* user)
            {}, -- request body 
            'vault/dev/aptos/node/mainnet'
        ) AS request
    FROM
        node_calls
)
SELECT
    PARSE_JSON(request):"data":"block_height"::STRING AS block_number,
    PARSE_JSON(request):"data":"block_hash"::STRING AS block_hash,
    PARSE_JSON(request):"data":"block_timestamp"::STRING AS block_timestamp_num,
    TO_TIMESTAMP(
        block_timestamp_num :: STRING
    ) AS block_timestamp,
    PARSE_JSON(request):"data":"first_version"::STRING AS first_version,
    PARSE_JSON(request):"data":"last_version"::STRING AS last_version,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp

FROM lq_calls;
-- SELECT request FROM lq_calls;
-- SELECT
--     REQUEST:DATA:BLOCK_HASH::STRING AS block_hash,
-- FROM LQ_CALLS;


SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height,
        CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0) AS batch_number
    FROM
        (
            SELECT
                block_number AS block_height
            FROM
                DATASCIENCE_DEV.streamline.aptos_blocks
            EXCEPT
            SELECT
                block_number
            FROM
                aptos.streamline.complete_blocks_tx
        )
    ORDER BY block_height DESC
    LIMIT 100;
            
SELECT
    max(block_number) 
FROM
    DATASCIENCE_DEV.streamline.aptos_blocks;

SELECT
    max(block_number)
FROM
    aptos.streamline.complete_blocks_tx;

WITH chainhead AS (
    SELECT live.udf_api( 
    'GET', 
    'https://fullnode.mainnet.aptoslabs.com/v1', 
    OBJECT_CONSTRUCT( 'Content-Type', 'application/json' ),{} ):data:block_height::INT as chainhead_blk;
)
SELECT
    max(_id) AS block_number
FROM
    crosschain_dev.silver.number_sequence
WHERE 
    _id < (SELECT chainhead_blk FROM chainhead);




use grail_data;

WITH block_range as (
        SELECT
            distinct t.value as block_height
        FROM (
            SELECT
                BRONZE.UDF_APTOS_CURRENT_HEIGHT() as current_block_number,
                177100659 as min_block_number,
                CASE
                    WHEN current_block_number - min_block_number > 5000 THEN min_block_number + 5000
                    ELSE current_block_number
                END as max_block_number,
                ARRAY_GENERATE_RANGE(min_block_number, max_block_number) as block_range
        ), TABLE(FLATTEN(input => block_range)) AS t(VALUE)
        ORDER BY 1 DESC
    ),
    node_calls AS (
        -- generate a list of URLs for API calls and assign a batch number to each
        SELECT * FROM (
            SELECT
                'https://twilight-silent-gas.aptos-mainnet.quiknode.pro/f64d711fb5881ce64cf18a31f796885050178031/v1/blocks/by_height/' || block_height || '?with_transactions=true' as calls,
                block_height,
                CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0) AS batch_number,
                ROW_NUMBER() OVER (ORDER BY block_height) AS rn
            FROM block_range
            ORDER BY block_height ASC
        ) sq
        WHERE
            rn <= 50
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
    ),
    lq_calls AS (
        SELECT
            CURRENT_TIMESTAMP(3) AS _inserted_timestamp,
            partition_key,
            t.value,
            grail_data_prod.live.udf_api(
                'GET', -- request method
                t.VALUE, -- request url
                {}, -- request headers
                {} -- request body
            ) AS resp
        FROM
            batches,
            TABLE(FLATTEN(input => calls)) AS t(VALUE)
    )
    SELECT * FROM lq_calls;

CREATE OR REPLACE EXTERNAL FUNCTION DATASCIENCE_DEV._LIVE.UDF_API("METHOD" VARCHAR(16777216), "URL" VARCHAR(16777216), "HEADERS" OBJECT, "DATA" VARIANT, "USER_ID" VARCHAR(16777216), "SECRET" VARCHAR(16777216))
RETURNS VARIANT
STRICT
API_INTEGRATION = "AWS_DATASCIENCE_API_STG"
MAX_BATCH_ROWS = 1
HEADERS = ('fsc-compression-mode' = 'auto')
AS 'https://65sji95ax3.execute-api.us-east-1.amazonaws.com/stg/udf_api';

desc function datascience_dev._live.UDF_API(VARCHAR, VARCHAR, OBJECT, VARIANT, VARCHAR, VARCHAR);