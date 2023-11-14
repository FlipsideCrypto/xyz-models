{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;

        {% endset %}
        {% do run_query(sql) %}
        {% if target.database != "APTOS_COMMUNITY_DEV" %}
            {% set sql %}
            {{ create_udf_bulk_json_rpc() }}
            {{ create_udf_bulk_rest_api() }}


            {% endset %}
            {% do run_query(sql) %}
        {% endif %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}