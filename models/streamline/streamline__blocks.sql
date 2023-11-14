{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% if execute %}
    {% set height = run_query('SELECT live.udf_api( ''GET'', ''https://twilight-silent-gas.aptos-mainnet.quiknode.pro/f64d711fb5881ce64cf18a31f796885050178031/v1'', OBJECT_CONSTRUCT( ''Content-Type'', ''application/json'' ),{} ) :data :block_height :: INT ') %}
    {% set block_height = height.columns [0].values() [0] %}
{% else %}
    {% set block_height = 0 %}
{% endif %}

SELECT
    _id AS block_number
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= {{ block_height }}
ORDER BY
    _id ASC
