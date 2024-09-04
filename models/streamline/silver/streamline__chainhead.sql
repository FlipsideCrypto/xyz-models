{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/v1',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery',
            'User-Agent',
            'Flipside_Crypto/0.1'
        ),
        OBJECT_CONSTRUCT(),
        'Vault/prod/m1/devnet'
    ) :data :block_height :: INT AS block_number
