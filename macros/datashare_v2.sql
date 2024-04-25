{% macro create_sp_share_build_and_grant_permissions_db() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_share_build_and_grant_permissions(
        db STRING
    ) returns TABLE (
        SQL STRING
    ) LANGUAGE SQL {% if target.name == "prod" %}
        EXECUTE AS owner
    {% else %}
        EXECUTE AS caller
    {% endif %}

    AS $$
DECLARE
    all_grants VARCHAR DEFAULT '';
BEGIN
    USE schema datashare;
show grants TO role velocity_ethereum;
CREATE
    OR REPLACE temporary TABLE datashare.grants AS
SELECT
    *
FROM
    TABLE(RESULT_SCAN(LAST_QUERY_ID()));
CREATE
    OR REPLACE temporary TABLE dbs AS
SELECT
    "name" AS db_name,
    'create share if not exists ' || db_name || ';' AS share_cmd,
    'alter share ' || db_name || ' set secure_objects_only = false;' AS non_sec_vw_cmd,
    'grant usage on database ' || db_name || ' to share ' || db_name || ';' AS db_usage_cmd
FROM
    grants
WHERE
    "granted_on" = 'DATABASE'
    AND db_name IN (
        -- This will soon be replaced by the table that will drive the frontend
        'APTOS',
        'ARBITRUM',
        'AURORA',
        'AVALANCHE',
        'AXELAR',
        'BASE',
        'BITCOIN',
        'BLAST',
        'BSC',
        'COSMOS',
        'ETHEREUM',
        'FLOW',
        'GNOSIS',
        'NEAR',
        'OPTIMISM',
        'OSMOSIS',
        'POLYGON',
        'SEI',
        'SOLANA',
        'TERRA',
        'THORCHAIN'
    );
CREATE
    OR REPLACE temporary TABLE schs AS
SELECT
    SPLIT_PART(
        "name",
        '.',
        1
    ) || '.' || SPLIT_PART(
        "name",
        '.',
        2
    ) schema_name,
    b.db_name,
    'grant usage on schema ' || schema_name || ' to share ' || db_name || ';' AS schema_usage_cmd,
    'grant select on all tables in schema ' || schema_name || ' to share ' || db_name || ';' AS table_select_cmd
FROM
    grants A
    JOIN dbs b
    ON SPLIT_PART(
        "name",
        '.',
        1
    ) = b.db_name
WHERE
    "granted_on" = 'SCHEMA'
    AND "name" NOT LIKE '%DBT%' -- There will soon be replaced by the table that will drive the frontend
    AND "name" NOT LIKE '%DEV%'
    AND "name" NOT LIKE '%NOT_NULL%'
    AND "name" NOT LIKE '%_INTERNAL%'
    AND "name" NOT LIKE '%UNIQUE%'
    AND "name" NOT LIKE '%SILVER%'
    AND "name" NOT LIKE 'LIVEQUERY%'
    AND "name" NOT LIKE '%TEST%'
    AND "name" NOT LIKE '%PUBLIC%'
    AND "name" NOT LIKE '%UTILS%'
    AND "name" NOT LIKE '%GITHUB%'
    AND "name" NOT LIKE '%STREAMLINE%'
    AND "name" NOT LIKE '%BRONZE%'
    AND "name" NOT LIKE '%.LIVE%'
    AND "name" NOT LIKE '%.BETA%';
CREATE
    OR REPLACE temporary TABLE vws AS
SELECT
    "name" AS table_name,
    b.schema_name,
    b.db_name,
    'grant select on view ' || "name" || ' to share ' || b.db_name || ';' AS view_select_cmd
FROM
    grants A
    JOIN schs b
    ON SPLIT_PART(
        "name",
        '.',
        1
    ) || '.' || SPLIT_PART(
        "name",
        '.',
        2
    ) = b.schema_name
WHERE
    "granted_on" IN ('VIEW')
    AND SPLIT_PART(
        "name",
        '.',
        3
    ) NOT LIKE 'SV%';
WITH share_grants AS (
        SELECT
            share_cmd AS cmd,
            db_name
        FROM
            dbs
        UNION ALL
        SELECT
            non_sec_vw_cmd,
            db_name
        FROM
            dbs
        UNION ALL
        SELECT
            db_usage_cmd,
            db_name
        FROM
            dbs
        UNION ALL
        SELECT
            schema_usage_cmd,
            db_name
        FROM
            schs
        UNION ALL
        SELECT
            table_select_cmd,
            db_name
        FROM
            schs
        UNION ALL
        SELECT
            view_select_cmd,
            db_name
        FROM
            vws
    )
SELECT
    LISTAGG(
        cmd,
        '\n'
    ) AS all_permissions INTO :all_grants
FROM
    share_grants
WHERE
    (LOWER(db_name) = LOWER(:db)
    OR LOWER(:db) = 'all');
IF (len(all_grants) > 0) THEN EXECUTE IMMEDIATE 'BEGIN\n' || :all_grants || 'END\n';END IF;
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

{% macro create_sp_share_build_and_grant_permissions() %}
    CREATE
    OR REPLACE PROCEDURE datashare.sp_share_build_and_grant_permissions() returns TABLE (
        table_catalog STRING
    ) LANGUAGE SQL {% if target.name == "prod" %}
        EXECUTE AS owner
    {% else %}
        EXECUTE AS caller
    {% endif %}

    AS $$
DECLARE
    results resultset;
BEGIN
    results:= (
        CALL {{ target.database }}.datashare.sp_share_build_and_grant_permissions('all')
    );
RETURN TABLE(results);
END;$$;
{% endmacro %}
