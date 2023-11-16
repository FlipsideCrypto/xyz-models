{% macro create_sp_create_streamline_users_roles_prod() -%}
CREATE
OR REPLACE PROCEDURE streamline.create_streamline_users_roles_prod(
    prod_db_name STRING,
    aws_lambda_prod_user_password STRING,
    dbt_cloud_prod_user_password STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    -- Use EXECUTE IMMEDIATE for DDL statements within a stored procedure
    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API PASSWORD = ''' || :aws_lambda_prod_user_password || '''';

    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API';

    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS DBT_CLOUD_' || :prod_db_name;

    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS dbt_cloud_' || :prod_db_name || ' PASSWORD = ''' || :dbt_cloud_prod_user_password || ''' DEFAULT_ROLE = dbt_cloud_' || :prod_db_name || ' MUST_CHANGE_PASSWORD = FALSE';

    RETURN 'Success: PROD users and roles created or updated successfully.';
END;$$;
{%- endmacro %}