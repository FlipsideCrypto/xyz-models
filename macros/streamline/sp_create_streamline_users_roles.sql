{% macro create_sp_create_streamline_users_roles() -%}
CREATE
OR REPLACE PROCEDURE streamline.create_streamline_users_roles(
    prod_db_name STRING,
    aws_lambda_user_password STRING,
    dbt_cloud_user_password STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    -- Use EXECUTE IMMEDIATE for DDL statements within a stored procedure
    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API PASSWORD = ''' || :aws_lambda_user_password || '''';

    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API';

    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS DBT_CLOUD_' || :prod_db_name;

    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS dbt_cloud_' || :prod_db_name || ' PASSWORD = ''' || :dbt_cloud_user_password || ''' DEFAULT_ROLE = dbt_cloud_' || :prod_db_name || ' MUST_CHANGE_PASSWORD = FALSE';

    RETURN 'Success: Users and roles created or updated successfully.';
END;$$;
{%- endmacro %}