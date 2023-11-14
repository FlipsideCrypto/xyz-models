{{ config (
    materialized = "view"
) }}

SELECT
    live.udf_api(
        'GET',
        'https://twilight-silent-gas.aptos-mainnet.quiknode.pro/{Authentication}/v1',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{}
    ) {# :data :block_height :: INT AS height #}
