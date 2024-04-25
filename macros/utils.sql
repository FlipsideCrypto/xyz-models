{% macro call_sp_grant_share_permissions(args) %}
    {% set sql %}
        call datashare.sp_grant_share_permissions({{ args }})
    {% endset %}
    {% if execute and not target.database.upper().endswith("_DEV") %}
        {% set results = run_query(sql) %}
        {% do log(sql, True) %}
        {% do results.print_table(max_column_width=255) %}
    {% endif %}
{% endmacro %}

{% macro call_sp_grant_share_permissions_v2() %}
    {% set sql %}
        call {{ target.database }}.datashare.sp_share_build_and_grant_permissions()
    {% endset %}
    {% if execute and not target.database.upper().endswith("_DEV") %}
        {% set results = run_query(sql) %}
        {% do log(sql, True) %}
        {% do results.print_table(max_column_width=255) %}
    {% endif %}
{% endmacro %}