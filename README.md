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

This project has been setup with `fsc_utils == 1.20.0` according to the instruction in the  [fsc_utils setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#adding-the-fsc_utils-dbt-package) documentation. In addition `livequery` models and schema has also been deployed to the `DATASCIENCE_DEV` database as per the instructions in the [livequery setup](https://github.com/FlipsideCrypto/fsc-utils?tab=readme-ov-file#livequery-functions) documentation.

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