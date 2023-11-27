{% docs __overview__ %}

# Welcome to the Flipside Crypto Aurora Models Documentation

## **What does this documentation cover?**

The documentation included here details the design of the Aurora
tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/aurora-models/)

## **How do I use these docs?**

The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`AURORA`.`CORE`.`<table_name>`)

**Fact Tables:**

- [fact_blocks](#!/model/model.aurora_models.core__fact_blocks)
- [fact_transactions](#!/model/model.aurora_models.core__fact_transactions)
- [fact_logs](#!/model/model.aurora_models.core__fact_logs)
- [fact_receipts](#!/model/model.aurora_models.core__fact_receipts)

## **Data Model Overview**

The Aurora models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted `ez_`) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

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

- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/aurora-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

## Archived Notices
### **⚠️ Aurora Data Notes and Known Issues**
*Final Update*  
We have switched to a node hosted by the Aurora team, and they pushed relevant fixes. The below issues are since resolved.  

*Update: July 27, 2023:*  
We have identified 5 primary issues with the data coming from the node, and a path forward to resolution after discussions with the Aurora Dev team:
1. Duplicated Transactions
 - Aurora Node issue, fix in progress by Aurora devs. Resolution timeline: 1-2 weeks
1. Reverted Transactions: Transactions or events that have been previously canceled or reverted are still being received from the chain.
 - Aurora Node issue, fix in progress by Aurora devs. Resolution timeline: 1-2 weeks
1. Inaccurate Data: We've detected some inaccuracies in the data, namely incorrect timestamps (10M first block set to 1970) and transactions appearing in the wrong blocks.
 - This is not a data integrity issue. Aurora contains pre-history and only launched as a public blockchain with **block 37,157,757**. Early blocks contain incomplete data, such as `0x0` as the block timestamp.
1. Incomplete Data: We've noticed certain transaction data missing, particularly with regards to received transactions. Incomplete data includes some blocks get read with wrong txs count and txs info there, we believed that is from the out of sync status of the node which needs a full backfill again when the node is back sync
 - This is likely due to our current node provider using an outdated version of the Aurora RPC package. Resolution: change node provider.
1. Block Confirmation Discrepancies: Transactions were confirmed on different blocks than those indicated in Explorer.
 - This is likely due to our current node provider using an outdated version of the Aurora RPC package. Resolution: change node provider.

Our plan of action is (likely) to move to a dedicated node provided by Aurora which will solve the 4 major issues with a single decision. This timeline is dependent on the patch by Aurora, and our timeline will be updated as we learn more.  


*Update: July 20, 2023:*  
In onboarding Aurora data, our team has encountered several issues with data returned from the node. These are primarily associated with transactions that are either reverted or cancelled. At present, the node returns these transactions across multiple blocks and in different positions within the block at each time. This is uncommon, as the position should be constant. We may see pending transactions within a block on other EVMs, but on re-request the transaction would be finalized. These seem to be persistent across multiple blocks, even in subsequent requests.  

At present, these transactions are included in our data. They will have `null` fields like status, fee, and others that are typically derived from receipts. These transactions do not have receipts, so we can identify them through their lack of receipt data.  

Flipside is working closely with Near and Aurora to determine how this data should best be presented.  

{% enddocs %}
