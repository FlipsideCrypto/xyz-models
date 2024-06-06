{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_tx",
        "sql_limit" :"1000000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"5000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks') }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks_tx_complete') }}
    ORDER BY
        block_number DESC
)
SELECT
    block_number,
    ROUND(
        block_number,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/v1/blocks/by_height/' || block_number || '?with_transactions=true',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        PARSE_JSON('{}'),
        'Vault/prod/m1/devnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
