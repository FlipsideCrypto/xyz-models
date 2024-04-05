{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
         params = {
            "external_table": "external_table",
            "sql_limit": "10",
            "producer_batch_size": "10",
            "worker_batch_size": "10",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['sportsdb']
) }}

-- This POC model showcases the push and pull mechanisms you can enable using a Quantum Model. Here we Use Livequery to `pull` all the goal scorers from the latest ice hockey games and then `push` work to streamline to ingest data about the goal scorers in batches using streamline.

-- Note: The post_hook will enable the push mechanism to streamline. The post_hook will call the udf_bulk_rest_api_v2 function to push work to streamline in batches. Refer to: https://github.com/FlipsideCrypto/streamline-flow/discussions/10#discussioncomment-7194378 for more information on how you can use the params to fan out worker lambdas for streamline.

WITH live_scores as (
    -- Get the latest ice hockey games
    -- Note: Setting the 'fsc-quantum-state' header to 'livequery' will enable us to "pull" the latest data from the API
    SELECT
        {{ target.database }}.live.udf_api(
            'GET',
            'https://{service}/api/v1/json/3/latesticehockey.php',
            {'fsc-quantum-state':'livequery'},
            {},
            'vault/stg/thesportsdb'
        ) AS calls
),
parsed_json AS (
    -- Parse games from the live scores
    SELECT
        VALUE AS games
    FROM
        live_scores,
        TABLE(
            FLATTEN(
                INPUT => PARSE_JSON(calls):data:games
            )
        )
),
goal_scorers AS (
    -- Get the goal scorer stats from each game
    SELECT
        VALUE:scorer AS scorer
    FROM
        parsed_json,
        TABLE(
            FLATTEN(
                INPUT => games:goals
            )
        )
),
api_calls AS (
    -- Get the player stats api url from the goal scorers in the latest games
    SELECT
        'https://{service}/api/v1/json/3/searchplayers.php?p=' || player as calls, 
        player
    FROM
        (
            SELECT
                distinct URL_ENCODE(scorer:player) AS player
            FROM
                goal_scorers
        )
)
SELECT
    DATE_PART('EPOCH', CURRENT_DATE())::INTEGER AS partition_key,
    -- Note: Setting the 'fsc-quantum-state' header to 'streamline' will enable us to "push" work to streamline
    {{ target.database }}.live.udf_api(
        'GET',
        calls,
        {'fsc-quantum-state':'streamline'},
        {},
        'vault/stg/thesportsdb'
    ) AS request
FROM
    api_calls