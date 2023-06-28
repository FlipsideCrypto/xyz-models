{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value(
        'BLOCKCHAIN_NAME',
        'BITCOIN'
    ) }}
      {{ set_database_tag_value(
        'BLOCKCHAIN_TYPE',
        'BITCOIN'
    ) }}
{% endmacro %}
