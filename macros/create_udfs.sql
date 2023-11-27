{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_json_rpc() }}

        {% endset %}
        {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}

    {% endif %}
{% endmacro %}
