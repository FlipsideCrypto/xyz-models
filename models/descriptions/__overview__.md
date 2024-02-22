{% docs __overview__ %}

# Welcome to the Flipside Crypto bitcoin Models Documentation

## **What does this documentation cover?**

The documentation included here details the design of the bitcoin
tables and views available via [Flipside Crypto.](https://flipsidecrypto.bitcoin/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/bitcoin-models/)

## **How do I use these docs?**

The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`bitcoin`.`CORE`.`<table_name>`)

**Fact Tables:**

- [fact_blocks](#!/model/model.bitcoin_models.core__fact_blocks)
- [fact_transactions](#!/model/model.bitcoin_models.core__fact_transactions)
- [fact_inputs](#!/model/model.bitcoin_models.core__fact_inputs)
- [fact_outputs](#!/model/model.bitcoin_models.core__fact_outputs)

**Dimensional Tables:**

- [dim_entity_clusters](#!/model/model.bitcoin_models.core__dim_entity_clusters)
- [dim_labels](#!/model/model.bitcoin_models.core__dim_labels)

### Governance Schema

- [ez_miner_rewards](#!/model/model.bitcoin_models.gov__ez_miner_rewards)

### Price Schema

- [fact_hourly_token_prices](#!/model/model.bitcoin_models.price__fact_hourly_token_prices)
- [ez_hourly_token_prices](#!/model/model.bitcoin_models.price__ez_hourly_token_prices)

**Stats EZ Tables:**

- [ez_core_metrics_hourly](#!/model/model.bitcoin_models.ez_core_metrics_hourly)

## **Data Model Overview**

The bitcoin
models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez\_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**

### Navigation

You can use the `Project` and `Database` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are _not_ shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--models` and `--exclude` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.

### **More information**

- [Flipside](https://flipsidecrypto.bitcoin/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/bitcoin-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}
