{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/latest/height',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/dev/aleo/mainnet'
    ) :data :: INT AS block_number
