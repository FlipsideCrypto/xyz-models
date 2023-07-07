{% macro create_sp_grant_share_permissions_string_timestamp_string() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(
        db STRING,
        updated_since timestamp_ntz,
        share_suffix STRING
    ) returns TABLE (
        SQL STRING
    ) LANGUAGE SQL EXECUTE AS caller AS $$
DECLARE
    share_name VARCHAR DEFAULT :db || :share_suffix;
ddl_views VARCHAR DEFAULT :db || '._datashare._create_gold';
target_schema VARCHAR DEFAULT :db || '.information_schema.schemata';
all_grants VARCHAR DEFAULT '';
BEGIN
    WITH target_schema AS (
        SELECT
            ARRAY_AGG(schema_name) AS schema_name
        FROM
            identifier(:target_schema)
    ),
    schema_grants AS (
        SELECT
            regexp_substr_all(
                ddl,
                '(__SOURCE__).([\\w]+)',
                1,
                1,
                'i',
                2
            ) AS extracted,
            array_distinct(SPLIT(UPPER(ARRAY_TO_STRING(extracted, ';')), ';')) AS distinct_schemata,
            array_intersection(
                ARRAY_APPEND(
                    distinct_schemata,
                    '_DATASHARE'
                ),
                (
                    SELECT
                        schema_name
                    FROM
                        target_schema
                )
            ) AS valid_schemata,
            REGEXP_REPLACE(
                ARRAY_TO_STRING(
                    valid_schemata,
                    ';'
                ),
                '([\\w]+)',
                '\nGRANT USAGE ON SCHEMA {DB}.\\1 TO SHARE {SHARE};' || '\nGRANT SELECT ON ALL TABLES IN SCHEMA {DB}.\\1 TO SHARE {SHARE}'
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
SELECT
    concat_ws(
        ' ',
        'CREATE SHARE IF NOT EXISTS',
        :share_name,
        ';\n',
        'GRANT USAGE ON DATABASE',
        :db,
        'TO SHARE',
        :share_name,
        ';\n',
        REPLACE(
            REPLACE(
                s.ddl,
                '{DB}',
                :db
            ),
            '{SHARE}',
            :share_name
        )
    ) AS permissions INTO :all_grants
FROM
    schema_grants s;
IF (
        all_grants IS NOT NULL
    ) THEN EXECUTE IMMEDIATE 'BEGIN\n' || :all_grants || 'END\n';
END IF;
let rs resultset:= (
    SELECT
        VALUE
    FROM
        TABLE(SPLIT_TO_TABLE(:all_grants, '\n'))
    WHERE
        VALUE NOT IN (
            '',
            ' '
        )
);
RETURN TABLE(rs);
END;$$;
{% endmacro %}

{% macro create_sp_grant_share_permissions_string_timestamp() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(
        db STRING,
        updated_since timestamp_ntz
    ) returns TABLE (
        SQL STRING
    ) LANGUAGE SQL EXECUTE AS caller AS $$
DECLARE
    results resultset;
BEGIN
    results:= (
        CALL datashare.sp_grant_share_permissions(
            :db,
            '2000-01-01' :: timestamp_ntz,
            '_FLIPSIDE_AF'
        )
    );
RETURN TABLE(results);
END;$$;
{% endmacro %}

{% macro create_sp_grant_share_permissions_timestamp() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(
        updated_since timestamp_ntz
    ) returns TABLE (
        table_catalog STRING
    ) LANGUAGE SQL AS $$
DECLARE
    cur CURSOR FOR
SELECT
    table_catalog,
    share_suffix
FROM
    snowflake.account_usage.tables
    CROSS JOIN (
        SELECT
            'FLIPSIDE_AF' AS share_suffix
        UNION ALL
        SELECT
            'FLIPSIDE_AF_PAID'
        UNION ALL
        SELECT
            'FLIPSIDE_AF_TRIAL'
    )
WHERE
    table_name = '_CREATE_GOLD'
    AND table_schema = '_DATASHARE'
    AND table_catalog {{ "" if target.database.upper().endswith("_DEV")
    ELSE "NOT" }} LIKE '%_DEV'
    AND table_catalog NOT LIKE '%_DEV'
GROUP BY
    1,
    2;
BEGIN
    CREATE
    OR REPLACE temporary TABLE results AS
SELECT
    '' :: STRING AS db
FROM
    dual
LIMIT
    0;
FOR cur_row IN cur DO let db VARCHAR:= cur_row.table_catalog;
let suffix VARCHAR:= cur_row.share_suffix;
CALL datashare.sp_grant_share_permissions(
        :db,
        :updated_since,
        :suffix
    );
let cnt VARCHAR:= (
        SELECT
            COUNT(*)
        FROM
            TABLE(RESULT_SCAN(LAST_QUERY_ID())));
IF (
                cnt > 0
            ) THEN
        INSERT INTO
            results (db)
        VALUES
            (:db);
    END IF;
END FOR;
let rs resultset:= (
    SELECT
        *
    FROM
        results
    ORDER BY
        db
);
RETURN TABLE(rs);
END;$$;
{% endmacro %}

{% macro create_sp_grant_share_permissions() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_grant_share_permissions() returns TABLE (
        SQL STRING
    ) LANGUAGE SQL EXECUTE AS caller AS $$
DECLARE
    results resultset;
BEGIN
    results:= (
        CALL datashare.sp_grant_share_permissions(
            :db,
            '2000-01-01' :: timestamp_ntz
        )
    );
RETURN TABLE(results);
END;$$;
{% endmacro %}

{% macro create_sp_grant_share_permissions_string() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_grant_share_permissions(
        db STRING
    ) returns TABLE (
        SQL STRING
    ) LANGUAGE SQL EXECUTE AS caller AS $$
DECLARE
    results resultset;
BEGIN
    results:= (
        CALL datashare.sp_grant_share_permissions(
            :db,
            '2000-01-01' :: timestamp_ntz
        )
    );
RETURN TABLE(results);
END;$$;
{% endmacro %}
