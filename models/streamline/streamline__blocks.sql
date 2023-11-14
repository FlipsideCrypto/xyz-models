{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% if execute %}
    {% set height = run_query('SELECT height from streamline.blocks_chainhead') %}
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
