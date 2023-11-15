{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'sql_limit', {{var('sql_limit','1200000')}}, 'producer_batch_size', {{var('producer_batch_size','300000')}}, 'worker_batch_size', {{var('worker_batch_size','50000')}}, 'sm_secret_name','prod/aptos/node/mainnet'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH calls AS (

    SELECT
        '{service}/{Authentication}/v1/transactions/by_version/' || tx_version calls,
        tx_version
    FROM
        (
            SELECT
                tx_version
            FROM
                {{ ref('streamline__transactions') }}
            EXCEPT
            SELECT
                tx_version
            FROM
                {{ ref('streamline__complete_transactions') }}
        )
)
SELECT
    ARRAY_CONSTRUCT(
        tx_version,
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
    tx_version DESC
