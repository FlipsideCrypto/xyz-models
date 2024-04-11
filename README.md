# Data Science Models - Quantum POC

This repo is a fork of the [FlistsideCrypto/xyz-models](https://github.com/FlipsideCrypto/xyz-models) repo, the `xyz-models` repo holds scafolding for DBT projects at `Flipside Crypto`.  This repo is intended to be used as a `POC` for showcasing the capabilities of using `Quantum Models` with a `streamine` backend wherein a single `DBT` model can be used for both `pull` and `push` based workloads. 

## Profile Set Up

#### Use the following within profiles.yml 
----

```yml
datascience:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: DATASCIENCE_DEV
      warehouse: DBT_CLOUD
      schema: silver
      threads: 4
      client_session_keep_alive: False
      query_tag: <TAG>
```

### Project setup

This project has been setup with `fsc_utils == 1.21.7` according to the instructions in the [fsc_utils setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#adding-the-fsc_utils-dbt-package) documentation. In addition `livequery` & `livequery marketplace GHA` models, schemas & functions have also been deployed to the `DATASCIENCE_DEV` database as per the instructions in the [livequery setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#livequery-functions) & [snowflake GHA tasks setup](https://github.com/flipsideCrypto/fsc-utils?tab=readme-ov-file#snowflake-tasks-for-github-actions) documentations.

## Invoking the POC Quantum model

You will find a `POC` `quantum` model at [models/streamline/quantum/streamline__sportsdb_live_scorers.sql](/models/streamline/quantum/streamline__sportsdb_live_scorers.sql)  

There is a `makefile` [directive](./Makefile#L2) for invoking the `POC` quantum model. To invoke the model, ensure you have your `~/.dbt/profiles.yml` setup according to the [profile setup](#profile-setup) instructions and run the following make command: 

```sh
make quantum-poc
``` 

### Understanding the `POC` quantum model

#### Bronze Layer

The [streamline__aptos_blocks_tx](/models/streamline/quantum/poc/core/streamline__aptos_blocks_tx.sql) model follows the desgn pattern of a `streamline` dbt model for ingesting data from a blockchain node, in this case the `Aptos Node API`. It uses a reusable [ephemeral model](/models/streamline/quantum/poc/core/streamline__aptos_blocks_tx_ephemeral.sql) that is designed to fetch data from the `Aptos Node API` endpoint for specific block heights that have not been fetched yet. Here's what the model does in more detail:

[node_calls CTE](/models/streamline/quantum/poc/core/streamline__aptos_blocks_tx_ephemeral.sql#L6): This `CTE` generates `REST` requests for the `Aptos Node API` endpoint based on block heights that have not been fetched yet. It does this by comparing the `block_number` from the `streamline__aptos_blocks` table and the `block_number` from the `aptos.streamline.complete_blocks_tx` table. The `EXCEPT` clause returns all `block_number` values that are in the `streamline__aptos_blocks` table but not in the `aptos.streamline.complete_blocks_tx` table. These are the block heights that have not been fetched yet. For each of these block heights, it generates a `REST` request `URL` and stores it in the `calls` column.

[Final SELECT Statement](/models/streamline/quantum/poc/core/streamline__aptos_blocks_tx_ephemeral.sql#L26): This statement fetches data from the API endpoint for each `REST` request URL generated in the `node_calls` CTE. It does this by calling the `live.udf_api` function with the `GET` method and the `calls` URL. The `live.udf_api` function is expected to return the response from the API endpoint. The final result includes the current timestamp as created_at and _inserted_timestamp, the block_height rounded to the nearest thousand as partition_key, and the API response as request.

**NOTE:** This model is intended to always run in `streamline mode` as it's purpose is to batch ingest data from the `Aptos Node`, as such being a quantum model, the mode is set based on who the "observer" is( in this case the `user` invoking the `livequery` AWS lambda ). Since this model is not schedule to run on a GHA cron, we spoof the user by setting the the user for this model through the a `dbt pre-hook` to set the session based context in the [dbt_project.yml file here](/dbt_project.yml#L40-46).

##### Invoking the bronze model

There is a make directive for invoking the bronze model. To invoke the model, ensure you have your `~/.dbt/profiles.yml` setup according to the [profile setup](#profile-setup) instructions and run the following make command:

```sh
make bronze
```
To view the results of the model (the work "pushed" to a streamline backend), run the following SQL query:

```sql
-- Set the snowflake console session context to the user invoking table
SET LIVEQUERY_CONTEXT = '{"userId":"aws_lambda_datascience_api"}';

SELECT * FROM DATASCIENCE_DEV.STREAMLINE.APTOS_BLOCKS_TX;
...


```

#### Silver Layer

Invoking `make silver` will run the `POC` quantum dbt [silver__blocks](/models/streamline/quantum/poc/silver/silver__blocks.sql) model. 

### Quantum Synergy

Once the `make silver` invocation is complete, this resulting `datascience_dev.silver__blocks` view will contain all the data from the `Aptos Node API` calls from the bronze layer and the delta `block_heights` that were not present at the time of model invocation in the `aptos.streamline.complete_blocks_tx` table pulled in via the `quantum state` livequery mode.

```sql 
SELECT * FROM DATASCIENCE_DEV.SILVER.BLOCKS;
```

### Enabling batching for Quantum models

When in `livequery` mode, you can enable batching for your quantum models by setting the [MAX_BATCH_ROWS](https://docs.snowflake.com/en/sql-reference/sql/create-external-function) param for the `live.udf_api` function. This will allow you to control the number of rows that are sent to the `livequery` backend via the `_live.udf_api` UDF.

This UDF is installed a part of the [livequery setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#livequery-functions). The default behaivor for `livequery function` setup is not to set `MAX_BATCH_ROWS` for the `live.udf_api` function. When it is not set Snowflake estimates the optimal batch size and uses that, this can cause timeouts from the livequery backend if the result set rows are too large to process for a AWS LAMBA.

To set the `MAX_BATCH_ROWS` for the `live.udf_api` function, you can run the following SQL command:


```sql
CREATE OR REPLACE EXTERNAL FUNCTION DATASCIENCE_DEV._LIVE.UDF_API("METHOD" VARCHAR(16777216), "URL" VARCHAR(16777216), "HEADERS" OBJECT, "DATA" VARIANT, "USER_ID" VARCHAR(16777216), "SECRET" VARCHAR(16777216))
RETURNS VARIANT
STRICT
API_INTEGRATION = "AWS_DATASCIENCE_API_STG"
MAX_BATCH_ROWS = 30
AS 'https://65sji95ax3.execute-api.us-east-1.amazonaws.com/stg/udf_api';
```
1. **node_calls CTE**: This CTE generates a list of `URLs` for `Aptos Node API` calls and assigns a batch number to each URL.

   It starts by selecting block numbers from the `streamline__aptos_blocks` table that are not already present in the `aptos.streamline.complete_blocks_tx` table. For each of these block numbers, it constructs a `URL` for an `Aptos Node API` call to get information about the block. 

   It also assigns a batch number to each `URL`, with up to 10 `URLs` per batch. This is done using the `CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0)` expression, which assigns a row number to each URL (ordered by block height in descending order), divides it by 10, and rounds up to the nearest integer. This effectively groups every 10 URLs into a batch.

   For example, if the `streamline__aptos_blocks` table contains block numbers `1` to `1000`, and the `aptos.streamline.complete_blocks_tx` table contains block numbers `1` to `900`, the `node_calls` CTE would generate URLs for block numbers `901` to `1000`, and assign a batch number to each `URL`, with batch numbers `1` to `10` for URLs `901` to `910`, batch numbers `11` to `20` for URLs `911` to `920`, and so on.

2. **batches CTE**: This CTE groups the URLs into batches and calculates a partition key for each batch.

   It does this by aggregating the `URLs` for each batch into an array using the `ARRAY_AGG(calls) AS calls` expression. It also calculates the average block number for each batch and rounds it to the nearest multiple of `1000` using the `ROUND(block_height,-3) AS partition_key` expression.

   For example, if batch `1` contains URLs for block numbers `901` to `910`, the `batches` CTE would aggregate these URLs into an array and calculate the partition key as `ROUND(905.5, -3) = 1000`. Similarly, if batch 2 contains URLs for block numbers 911 to 920, the `batches` CTE would aggregate these URLs into an array and calculate the partition key as `ROUND(915.5, -3) = 1000`, and so on.

These two `CTEs` are preparing the data for making batched `Livequery API` calls. The `node_calls` CTE generates the URLs for the API calls and assigns a batch number to each URL, and the `batches` CTE groups the URLs into batches and calculates a partition key for each batch.

The `lq_calls` CTE is designed to make API calls for each URL in the batches and store the results in a logical table (CTE resultset). It does this by unnesting the array of `URLs` for each batch and making an `API` call via `live.udf_api`.

The `TABLE(FLATTEN(input => calls)) AS t(VALUE)` part of the query is where the unnesting happens. The `FLATTEN` function is used to transform a semi-structured data type (like an array) into a relational representation. In this case, it's being used to unnest the calls array from the batches CTE.

The `input => calls` argument tells `FLATTEN` to unnest the calls array. The `FLATTEN` function returns a table with a single column named `VALUE`. This column contains the unnested elements of the array. The AS `t(VALUE)` part of the query renames this column to VALUE.

For example, if the batches CTE produces the following table:

| batch_number | calls                  | partition_key |
|--------------|------------------------|---------------|
| 1            | ['url1', 'url2', 'url3'] | 1000        |
| 2            | ['url4', 'url5', 'url6'] | 2000        |

The `TABLE(FLATTEN(input => calls))` AS `t(VALUE)` part of the query would unnest the calls array and produce the following table:

| VALUE | partition_key |
|-------|---------------|
| url1  | 1000          |
| url2  | 1000          |
| url3  | 1000          |
| url4  | 2000          |
| url5  | 2000          |
| url6  | 2000          |

The other part of the model is creating a SQL `SELECT` statement to extract data from a source table, transform it, and load it into a target table. Here's a breakdown of what this part of the model does:

1. `SELECT * FROM (...)`: This is the outer query that selects all columns from the result of the inner query.

2. The inner query selects several columns from a source table. The source table is either `bronze__streamline_blocks_tx` or `bronze__streamline_FR_blocks_tx`, depending on whether the model is being run in incremental mode.

3. `partition_key`, `block_number`, `block_hash`, `block_timestamp_num`, `block_timestamp`, `first_version`, `last_version`, `inserted_timestamp`, `modified_timestamp`, `_inserted_timestamp`, `_invocation_id`, and `source` are the columns being selected. Some of these columns are derived from the `DATA` column in the source table, which appears to be a JSON object.

4. `TO_TIMESTAMP(block_timestamp_num :: STRING) AS block_timestamp`: This line is converting the `block_timestamp_num` column to a string and then converting that string to a timestamp.

5. `{% if is_incremental() %}`: This is a dbt conditional that checks if the model is being run in incremental mode. If it is, the model selects from `bronze__streamline_blocks_tx` and only includes rows where `_inserted_timestamp` is greater than the maximum `_inserted_timestamp` in the target table. If the model is not being run in incremental mode, it selects from `bronze__streamline_FR_blocks_tx` without any additional filtering.

6. `qualify(ROW_NUMBER() over(PARTITION BY block_number ORDER BY _inserted_timestamp DESC)) = 1`: This line is a window function that partitions the data by `block_number` and orders it by `_inserted_timestamp` in descending order. The `QUALIFY` clause then filters the result to only include the first row in each partition (i.e., the row with the most recent `_inserted_timestamp` for each `block_number`).


**Note:** For more details on using the `udf params` used in streamline mode `post_hooks` refer to the following: 
 - [Lessons learned tuning backfills ](https://github.com/FlipsideCrypto/streamline-flow/discussions/10#discussioncomment-7194378)  
 - [Optimizing backfill tuning Streamline models](https://flipsidecrypto.slack.com/docs/T6F1AJ69E/F05V71L3ZJS)
 - [Streamline architecture overview](https://github.com/flipsideCrypto/streamline?tab=readme-ov-file#architecture-overview) 

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

## Applying Model Tags

### Database / Schema level tags

Database and schema tags are applied via the `add_database_or_schema_tags` macro.  These tags are inherited by their downstream objects.  To add/modify tags call the appropriate tag set function within the macro.

```
{{ set_database_tag_value('SOME_DATABASE_TAG_KEY','SOME_DATABASE_TAG_VALUE') }}
{{ set_schema_tag_value('SOME_SCHEMA_TAG_KEY','SOME_SCHEMA_TAG_VALUE') }}
```

### Model tags

To add/update a model's snowflake tags, add/modify the `meta` model property under `config`.  Only table level tags are supported at this time via DBT.

```
{{ config(
    ...,
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'SOME_PURPOSE'
            }
        }
    },
    ...
) }}
```

By default, model tags are not pushed to snowflake on each load.  You can push a tag update for a model by specifying the `UPDATE_SNOWFLAKE_TAGS` project variable during a run.

```
dbt run --var '{"UPDATE_SNOWFLAKE_TAGS":True}' -s models/core/core__fact_swaps.sql
```

### Querying for existing tags on a model in snowflake

```
select *
from table(datascience.information_schema.tag_references('datascience.core.fact_blocks', 'table'));
```
