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

SELECT *
FROM {{ ref('streamline__aptos_blocks_tx_ephemeral') }}
