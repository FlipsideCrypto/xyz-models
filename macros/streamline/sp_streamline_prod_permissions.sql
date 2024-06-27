{% macro create_sp_streamline_prod_permissions() -%}
CREATE OR REPLACE PROCEDURE streamline.streamline_prod_permissions(prod_db_name STRING)
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    BEGIN TRANSACTION;

    EXECUTE IMMEDIATE 'CREATE DATABASE IF NOT EXISTS ' || :prod_db_name;
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :prod_db_name || '.BRONZE';
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :prod_db_name || '.SILVER';
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :prod_db_name || '.STREAMLINE';
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :prod_db_name || '._INTERNAL';
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS STREAMLINE.' || :prod_db_name;


    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE ' || :prod_db_name || ' TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';

    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON FUTURE SCHEMAS IN DATABASE ' || :prod_db_name || ' TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name;
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name;
    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ' || :prod_db_name || '.STREAMLINE TO ROLE DBT_CLOUD_' || :prod_db_name;

    EXECUTE IMMEDIATE 'GRANT OWNERSHIP ON DATABASE ' || :prod_db_name || ' TO ROLE DBT_CLOUD_' || :prod_db_name || ' COPY CURRENT GRANTS';

    COMMIT;
    RETURN TRUE;
END;
$$;
{%- endmacro %}