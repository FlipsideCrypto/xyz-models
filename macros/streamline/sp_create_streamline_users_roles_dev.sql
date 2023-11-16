{% macro create_sp_create_streamline_users_roles_dev() -%}
CREATE
OR REPLACE PROCEDURE streamline.create_streamline_users_roles_dev(
    prod_db_name STRING,
    aws_lambda_dev_user_password STRING,
    dbt_cloud_dev_user_password STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    -- Use EXECUTE IMMEDIATE for DDL statements within a stored procedure
    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API_DEV PASSWORD = ''' || :aws_lambda_dev_user_password || '''';

    EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS aws_lambda_' || :prod_db_name || '_API_DEV';

    RETURN 'Success: DEV users and roles created or updated successfully.';
END;$$;
{%- endmacro %}