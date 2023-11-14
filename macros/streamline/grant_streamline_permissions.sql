{% macro grant_streamline_permissions(project, warehouse_name, integration_name, dev_integration_name, lambda_role, dbt_cloud_role, internal_dev_role) %}

{% set db_name = project %}
{% set dev_db_name = project + 'dev' %}

grant role {{ lambda_role }} to user {{ lambda_role }};
grant role {{ internal_dev_role }} to role {{ lambda_role }};
grant usage on warehouse {{ warehouse_name }} to role {{ lambda_role }};
grant role {{ dbt_cloud_role }} to role ACCOUNTADMIN;
grant role {{ internal_dev_role }} to role {{ dbt_cloud_role }};
grant usage on integration {{ integration_name }} to role {{ dbt_cloud_role }};
grant usage on integration {{ integration_name }} to role {{ lambda_role }};
grant usage on integration {{ integration_name }} to role {{ internal_dev_role }};
grant create integration on account to role {{ dbt_cloud_role }};
grant usage on integration {{ dev_integration_name }} to role {{ dbt_cloud_role }};
grant usage on integration {{ dev_integration_name }} to role {{ lambda_role }};
grant usage on integration {{ dev_integration_name }} to role {{ internal_dev_role }};
grant usage on warehouse {{ warehouse_name }} to role {{ dbt_cloud_role }};
grant create database on account to role {{ dbt_cloud_role }};
grant manage grants on account to role {{ dbt_cloud_role }};
grant execute task on account to role {{ dbt_cloud_role }};

--External Table Permissions
grant role {{ dbt_cloud_role }} to user {{ dbt_cloud_role }};
grant usage on stage streamline.bronze.external_tables to role {{ dbt_cloud_role }};
grant usage on stage streamline.bronze.external_tables to role {{ lambda_role }};

--Internal_Dev Prod Permissions
grant usage on all schemas in database {{ db_name }} to role {{ internal_dev_role }};
grant usage on future schemas in database {{ db_name }} to role {{ internal_dev_role }};
grant usage on all functions in database {{ db_name }} to role {{ internal_dev_role }};
grant usage on future functions in database {{ db_name }} to role {{ internal_dev_role }};
grant select on all tables in database {{ db_name }} to role {{ internal_dev_role }};
grant select on future tables in database {{ db_name }} to role {{ internal_dev_role }};
grant select on all views in database {{ db_name }} to role {{ internal_dev_role }};
grant select on future views in database {{ db_name }} to role {{ internal_dev_role }};

-- The following will grant permissions to the specified roles for both `project` and `project_dev` databases:
{% for suffix in ['', '_dev'] %}
    grant usage on database {{ db_name + suffix }} to role {{ lambda_role }};
    grant usage on database {{ db_name + suffix }} to role {{ dbt_cloud_role }};
    grant usage on database {{ db_name + suffix }} to role {{ internal_dev_role }};

    grant select on all views in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};
    grant select on all tables in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};
    grant usage on all functions in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};
    grant select on future views in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};
    grant select on future tables in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};
    grant usage on future functions in schema {{ db_name + suffix }}.streamline to role {{ lambda_role }};

    grant usage on schema streamline.{{ db_name + suffix }} to role {{ lambda_role }};
    grant select on all tables in schema streamline.{{ db_name + suffix }} to role {{ lambda_role }};
    grant select on all views in schema streamline.{{ db_name + suffix }} to role {{ lambda_role }};
    grant select on future tables in schema streamline.{{ db_name + suffix }} to role {{ lambda_role }};
    grant select on future views in schema streamline.{{ db_name + suffix }} to role {{ lambda_role }};

    grant usage on schema streamline.{{ db_name + suffix }} to role {{ dbt_cloud_role }};
    grant select on all tables in schema streamline.{{ db_name + suffix }} to role {{ dbt_cloud_role }};
    grant select on all views in schema streamline.{{ db_name + suffix }} to role {{ dbt_cloud_role }};
    grant select on future tables in schema streamline.{{ db_name + suffix }} to role {{ dbt_cloud_role }};
    grant select on future views in schema streamline.{{ db_name + suffix }} to role {{ dbt_cloud_role }};

    grant usage on schema streamline.{{ db_name + suffix }} to role {{ internal_dev_role }};
    grant select on all tables in schema streamline.{{ db_name + suffix }} to role {{ internal_dev_role }};
    grant select on all views in schema streamline.{{ db_name + suffix }} to role {{ internal_dev_role }};
    grant select on future tables in schema streamline.{{ db_name + suffix }} to role {{ internal_dev_role }};
    grant select on future views in schema streamline.{{ db_name + suffix }} to role {{ internal_dev_role }};

{% endfor %}

{% endmacro %}