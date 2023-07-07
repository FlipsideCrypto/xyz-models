{% macro call_sp_grant_share_permissions(args) %}
    {% set sql %}
        call datashare.sp_grant_share_permissions({{ args }})
    {% endset %}
    {% if execute %}
        {% set results = run_query(sql) %}
        {% do log(sql, True) %}
        {% do results.print_table(max_column_width=255) %}
    {% endif %}
{% endmacro %}