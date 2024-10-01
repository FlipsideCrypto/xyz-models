{% macro create_sps() %}
    {% if target.database == 'aleo' %}
        CREATE SCHEMA IF NOT EXISTS _internal;
        {{ sp_create_prod_clone('_internal') }};
    {% endif %}
{% endmacro %}

{% macro enable_search_optimization(schema_name, table_name, condition = '') %}
    {% if target.database == 'aleo' %}
        ALTER TABLE {{ schema_name }}.{{ table_name }} ADD SEARCH OPTIMIZATION {{ condition }}
    {% endif %}
{% endmacro %}