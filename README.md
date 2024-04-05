# DATASCIENCE DBT Project

This repo is a fork of the [FlistsideCrypto/xyz-models](https://github.com/FlipsideCrypto/xyz-models) repo, the `xyz-models` repo holds scafolding for DBT projects at `Flipside Crypto`.  `datascience-models` is intended to be used as a `POC` for showcasing the capabilities of using `Quantum Models` with a `streamine` backend wherein a single `DBT` model can be used for both `pull` and `push` based workloads. 

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
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 4
      client_session_keep_alive: False
      query_tag: <TAG>
```

### Project setup

This project has been setup with `fsc_utils == 1.21.7` according to the instructions in the  [fsc_utils setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#adding-the-fsc_utils-dbt-package) documentation. In addition `livequery` & `livequery marketplace GHA` models, schemas & functions has also been deployed to the `DATASCIENCE_DEV` database as per the instructions in the [livequery setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#livequery-functions) & [snowflake GHA tasks setup](https://github.com/flipsideCrypto/fsc-utils?tab=readme-ov-file#snowflake-tasks-for-github-actions) documentations.

## Invoking the POC Quantum model

You will find a `POC` `quantum` model in [models/streamline/quantum/streamline__sportsdb_live_scorers.sql](/models/streamline/quantum/streamline__sportsdb_live_scorers.sql).  

This model is intended to be used as a `POC` for showcasing the capabilities of using `Quantum Models` with a `streamine` backend wherein a single `DBT` model can be used for both `pull` and `push` based workloads. 

There is a `makefile` [directive](./Makefile#L2) for invoking the `POC` quantum model. To invoke the model, run the following make command `make quantum-poc`. Invoking `make quantum-poc` will run the `POC` quantum model and the output will be as follows :

```sh
make poc

03:45:36  Running with dbt=1.7.10
03:45:36  Registered adapter: snowflake=1.7.0
03:45:37  Found 85 models, 2 seeds, 5 operations, 5 analyses, 130 tests, 9 sources, 0 exposures, 0 metrics, 975 macros, 0 groups, 0 semantic models
03:45:37  
03:45:41  
03:45:41  Running 3 on-run-start hooks
03:45:41  1 of 3 START hook: datascience_models.on-run-start.0 ........................... [RUN]
03:45:41  1 of 3 OK hook: datascience_models.on-run-start.0 .............................. [OK in 0.00s]
03:45:41  2 of 3 START hook: datascience_models.on-run-start.1 ........................... [RUN]
03:45:41  2 of 3 OK hook: datascience_models.on-run-start.1 .............................. [OK in 0.00s]
03:45:41  3 of 3 START hook: livequery_models.on-run-start.0 ............................. [RUN]
03:45:41  3 of 3 OK hook: livequery_models.on-run-start.0 ................................ [OK in 0.00s]
03:45:41  
03:45:41  Concurrency: 12 threads (target='dev')
03:45:41  
03:45:41  1 of 1 START sql view model streamline.sportsdb_live_scorers ................... [RUN]
03:45:41  Running macro `if_data_call_function`: Calling udf udf_bulk_rest_api_v2 with params: 
{
  "external_table": "external_table",
  "producer_batch_size": "10",
  "sql_limit": "10",
  "sql_source": "{{this.identifier}}",
  "worker_batch_size": "10"
}
 on {{this.schema}}.{{this.identifier}}
03:45:50  1 of 1 OK created sql view model streamline.sportsdb_live_scorers .............. [SUCCESS 1 in 9.13s]
03:45:50  
03:45:50  Running 2 on-run-end hooks
03:45:50  1 of 2 START hook: datascience_models.on-run-end.0 ............................. [RUN]
03:45:50  1 of 2 OK hook: datascience_models.on-run-end.0 ................................ [OK in 0.00s]
03:45:50  2 of 2 START hook: livequery_models.on-run-end.0 ............................... [RUN]
03:45:50  2 of 2 OK hook: livequery_models.on-run-end.0 .................................. [OK in 0.00s]
03:45:50  
03:45:50  
03:45:50  Finished running 1 view model, 5 hooks in 0 hours 0 minutes and 13.73 seconds (13.73s).
03:45:50  
03:45:50  Completed successfully
03:45:50  
03:45:50  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```
**Note:** For more details on using the `udf params` in the models post hook refer to this [discussion](https://github.com/FlipsideCrypto/streamline-flow/discussions/10#discussioncomment-7194378).

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

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