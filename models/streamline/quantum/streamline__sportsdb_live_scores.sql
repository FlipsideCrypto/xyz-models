{{ config (
    materialized = "view",
    tags = ['sportsdb']
) }}
SELECT
    live.udf_api(
        'GET',
        'https://{service}/api/v1/json/3/latesticehockey.php',
        {},
        {},
        'vault/stg/thesportsdb'
    ) AS resp;