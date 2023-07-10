{% macro create_sp_grant_share_permissions_string_timestamp() %}
CREATE OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(db STRING, updated_since TIMESTAMP_NTZ)
RETURNS TABLE (SQL STRING)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  ddl_views VARCHAR DEFAULT :db || '._datashare._create_gold';
  target_schema VARCHAR DEFAULT :db || '.information_schema.schemata';
  all_grants VARCHAR DEFAULT '';
BEGIN
    WITH target_schema AS (
        SELECT
            array_agg(schema_name) AS schema_name
        FROM identifier(:target_schema)
    ),
    schema_grants AS (
        SELECT
            regexp_substr_all(ddl,'(__SOURCE__).([\\w]+)', 1, 1,'i', 2) AS extracted
            , array_distinct(split(upper(array_to_string(extracted, ';')), ';')) AS distinct_schemata
            , array_intersection(
                array_append(distinct_schemata, '_DATASHARE'),
                (SELECT schema_name FROM target_schema)
            ) AS valid_schemata,
             regexp_replace(
                array_to_string(valid_schemata, ';'),
                '([\\w]+)',
                '\nGRANT USAGE ON SCHEMA {DB}.\\1 TO SHARE {SHARE};' ||
                '\nGRANT SELECT ON ALL TABLES IN SCHEMA {DB}.\\1 TO SHARE {SHARE}'
            ) || ';\n' AS ddl
        FROM
            identifier(:ddl_views)
        WHERE
            ddl_created_at >= :updated_since
        ORDER BY
            ddl_created_at DESC
        LIMIT
            1
    )
    ,add_suffix as (
    SELECT concat_ws(' ',
        'CREATE SHARE IF NOT EXISTS', :db || suffix, ';\n',
        'GRANT USAGE ON DATABASE',:db, 'TO SHARE', :db || suffix, ';\n',
        replace(
            replace(
                s.ddl,
                '{DB}',
                :db),
            '{SHARE}',
            :db || suffix)
        ) AS permissions
    FROM schema_grants s
    CROSS JOIN (SELECT suffix from datashare.share_suffix where is_active = true)
    )
    SELECT listagg(permissions,'\n') as all_permissions
            INTO :all_grants
            FROM add_suffix;

    IF (all_grants IS NOT NULL) THEN
      EXECUTE IMMEDIATE 'BEGIN\n' || :all_grants || 'END\n';
    END IF;
    LET rs RESULTSET := (
        SELECT value
        FROM TABLE(split_to_table(:all_grants,'\n'))
        WHERE value NOT IN  ('', ' ')
      );
  RETURN TABLE(rs);
END;
$$
;
{% endmacro %}

{% macro create_sp_grant_share_permissions_timestamp() %}
CREATE OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(updated_since TIMESTAMP_NTZ)
RETURNS TABLE (TABLE_CATALOG STRING)
LANGUAGE SQL
AS
$$
DECLARE
    cur CURSOR FOR SELECT
        table_catalog
    FROM snowflake.account_usage.tables
    WHERE table_name = '_CREATE_GOLD'
    and table_schema = '_DATASHARE'
    AND TABLE_CATALOG NOT LIKE '%_DEV';
BEGIN
    create or replace temporary table results as
    SELECT ''::STRING AS db
    FROM dual
    LIMIT 0;
    FOR cur_row IN cur DO
        LET db VARCHAR := cur_row.table_catalog;
        CALL datashare.sp_grant_share_permissions(:db, :updated_since);
        LET cnt VARCHAR := (SELECT COUNT(*) FROM TABLE(result_scan(last_query_id())));
        IF (cnt > 0) THEN
            INSERT INTO RESULTS (db) VALUES (:db);
        END IF;
    END FOR;
    LET rs RESULTSET := (SELECT * FROM results ORDER BY db);
    RETURN TABLE(rs);
END;
$$
;
{% endmacro %}

{% macro create_sp_grant_share_permissions() %}
CREATE OR REPLACE PROCEDURE datashare.sp_grant_share_permissions()
RETURNS TABLE (SQL STRING)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  results RESULTSET;
BEGIN
  results := (CALL datashare.sp_grant_share_permissions(:db, '2000-01-01'::TIMESTAMP_NTZ) );
  RETURN TABLE(results);
END;
$$
;
{% endmacro %}

{% macro create_sp_grant_share_permissions_string() %}
CREATE OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(db STRING)
RETURNS TABLE (SQL STRING)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  results RESULTSET;
BEGIN
  results := (CALL datashare.sp_grant_share_permissions(:db, '2000-01-01'::TIMESTAMP_NTZ) );
  RETURN TABLE(results);
END;
$$
;
{% endmacro %}