{% macro create_sps() %}
    {% set sql %}
        {% if target.database == 'admin' %}
            CREATE SCHEMA IF NOT EXISTS _internal;
            {{ sp_create_prod_clone('_internal') }};

        {% endif %}

        CREATE SCHEMA IF NOT EXISTS datashare;
        {{ create_sp_grant_share_permissions_string_timestamp() }}
        {{ create_sp_grant_share_permissions_timestamp() }}
        {{ create_sp_grant_share_permissions() }}
        {{ create_sp_grant_share_permissions_string() }}
        {{create_sp_share_build_and_grant_permissions_db() }}
        {{create_sp_share_build_and_grant_permissions() }}

        CREATE SCHEMA IF NOT EXISTS streamline;
        {{create_sp_create_streamline_users_roles_dev() }}
        {{create_sp_create_streamline_users_roles_prod() }}
        {{create_sp_streamline_dev_permissions() }}
        {{create_sp_streamline_prod_permissions() }}

    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}