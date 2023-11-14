{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% if execute %}
    WITH chainhead AS (

        SELECT
            {{ target.database }}.live.udf_api(
                'GET',
                'https://twilight-silent-gas.aptos-mainnet.quiknode.pro/f64d711fb5881ce64cf18a31f796885050178031/v1',
                OBJECT_CONSTRUCT(
                    'Content-Type',
                    'application/json'
                ),{}
            ) :data :block_height :: INT AS block_height
    )
SELECT
    _id AS block_number
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            block_height
        FROM
            chainhead
    )
{% else %}
SELECT
    0 AS block_number
{% endif %}
