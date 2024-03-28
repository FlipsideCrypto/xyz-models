{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {{ create_aws_datascience_api() }}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}