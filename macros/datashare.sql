{% macro create_sp_grant_share_permissions_string_timestamp() %}
create or replace procedure datashare.sp_grant_share_permissions(db string, updated_since TIMESTAMP_NTZ)
returns table (sql string)
LANGUAGE SQL
execute as caller
as
$$
DECLARE
  share_name varchar default :db || '_FLIPSIDE_AF';
  ddl_views varchar default :db || '._datashare._create_gold';
  target_schema varchar default :db || '.information_schema.schemata';
  all_grants varchar default '';
BEGIN
    with target_schema as (
        select
            array_agg(schema_name) as schema_name
        from identifier(:target_schema)
    ),
    schema_grants as (
        select
            regexp_substr_all(ddl,'(__SOURCE__).([\\w]+)', 1, 1,'i', 2) as extracted
            , array_distinct(split(upper(array_to_string(extracted, ';')), ';')) as distinct_schemata
            , array_intersection(
                array_append(distinct_schemata, '_DATASHARE'),
                (select schema_name from target_schema)
            ) as valid_schemata,
             regexp_replace(
                array_to_string(valid_schemata, ';'),
                '([\\w]+)',
                '\nGRANT USAGE ON SCHEMA {DB}.\\1 TO SHARE {SHARE};' ||
                '\nGRANT SELECT ON ALL TABLES IN SCHEMA {DB}.\\1 TO SHARE {SHARE}'
            ) || ';\n' as ddl
        from
            identifier(:ddl_views)
        where
            ddl_created_at >= :updated_since
        order by
            ddl_created_at desc
        limit
            1
    )
    select concat_ws(' ',
        'CREATE SHARE IF NOT EXISTS', :share_name, ';\n',
        'GRANT USAGE ON DATABASE',:db, 'TO SHARE', :share_name, ';\n',
        replace(
            replace(
                s.ddl,
                '{DB}',
                :db),
            '{SHARE}',
            :share_name)
        ) as permissions
     into :all_grants
    from schema_grants s;

    IF (all_grants is not null) then
      EXECUTE IMMEDIATE 'BEGIN\n' || :all_grants || 'END\n';
  end if;
  let rs resultset := (select value
      from table(split_to_table(:all_grants,'\n'))
      where value not in  ('', ' '));
  RETURN table(rs);
END;
$$
;
{% endmacro %}

{% macro create_sp_grant_share_permissions_timestamp() %}
CREATE OR REPLACE procedure datashare.sp_grant_share_permissions(updated_since TIMESTAMP_NTZ)
returns table (TABLE_CATALOG string)
LANGUAGE SQL
as
$$
DECLARE
  cur CURSOR FOR  select
        table_catalog
    from snowflake.account_usage.tables
    where table_name = '_CREATE_GOLD'
    and table_schema = '_DATASHARE'
    AND TABLE_CATALOG not like '%_DEV';
BEGIN
    create or replace temporary table results as
    select ''::string as db
    from dual
    limit 0;
    FOR cur_row IN cur DO
        let db varchar:= cur_row.table_catalog;
        call datashare.sp_grant_share_permissions(:db, :updated_since);
        let cnt varchar := (select count(*) from table(result_scan(last_query_id())));
        if (cnt > 0) then
            insert into results (db) values (:db);
        end if;
    END FOR;
    let rs resultset := (select * from results order by db);
    RETURN TABLE(rs);
END;
$$
;
{% endmacro %}

{% macro create_sp_grant_share_permissions() %}
create or replace procedure datashare.sp_grant_share_permissions()
returns table (sql string)
LANGUAGE SQL
execute as caller
as
$$
DECLARE
  results resultset;
BEGIN
  results := (call datashare.sp_grant_share_permissions(:db, '2000-01-01'::TIMESTAMP_NTZ) );
  RETURN table(results);
END;
$$
;
{% endmacro %}