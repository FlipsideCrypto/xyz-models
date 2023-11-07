import pandas as pd
import networkx as nx
import numpy as np
import datetime


def model(dbt, session):

    dbt.config(
        materialized="table",
        packages=["networkx"],
        unique_key="ADDRESS",
        tags=['entity_cluster']
    )


    ########################
    #### NET NEW TXS QUERY
    ########################
    btc_dat = dbt.ref("silver__incremental_address_txs")
    btc_dat = btc_dat.to_pandas()

    ## STRIP AND REMOVE THE FIRST IN THE GROUP
    btc_dat["ADDRESS_ARRAY"] = btc_dat["ADDRESS_ARRAY"].str.strip('[]').str.replace('\"', '').str.split(',')
    btc_dat["ADDRESS_ARRAY"] = pd.array(btc_dat["ADDRESS_ARRAY"])

    btc_dat["FIRST"] = btc_dat["ADDRESS_ARRAY"].apply(lambda x: x.pop(0))

    # ### EXPLODE OUT DATA
    btc_dat_e = btc_dat.explode('ADDRESS_ARRAY')
    btc_dat_e = btc_dat_e[["ADDRESS_ARRAY", "FIRST"]]
    btc_dat_e = btc_dat_e.drop_duplicates()


    ########################
    ###### Base Set
    ########################
    btc_base_tab = dbt.ref("silver__incremental_base_clusters")
    btc_base_tab = btc_base_tab.to_pandas()

    # convert to an array
    btc_base_tab["ADDRESS_LIST"] = btc_base_tab["ADDRESS_LIST"].str.strip('[]').str.replace('\"', '').str.split(',')
    btc_base_tab["ADDRESS_LIST"] = pd.array(btc_base_tab["ADDRESS_LIST"])

    ## adding dashes as indicator of group
    btc_base_tab["ADDRESS_GROUP"] = btc_base_tab["ADDRESS_GROUP"].apply(lambda x: "--" + str(x))

    #exploding out addresses
    btc_base_tab_e = btc_base_tab.explode('ADDRESS_LIST')
    btc_base_tab_e.columns = ["ADDRESS_ARRAY", "FIRST"]

    #concat for modeling
    model_dat = pd.concat([btc_base_tab_e, btc_dat_e])


    ########################
    #### Network X for full data!! 
    ########################
    G = nx.from_pandas_edgelist(model_dat, 'FIRST','ADDRESS_ARRAY')
    edges = [i for i in nx.connected_components(G)]


    ## THIS IS FINAL DATA
    output_list = list(map(str, edges))
    final_dat = pd.DataFrame(output_list, columns = ['connections'])
    final_dat["connections_array"] = pd.array(final_dat["connections"].str.strip('{}').str.replace(' ','').str.replace("'",'').str.split(','))


    ########################
    ###### Max ID for ID generation
    ########################
    max_id = dbt.ref("silver__incremental_max_cluster")
    max_id = max_id.to_pandas()

    max_group_id = max_id["MAX_GROUP_ID"].iloc[0]


    ########################
    #### LOOPING FOR NEW CLUSTERS/MERGES/ETC.
    ########################

    #pulling out clusters and addresses
    final_dat["clusters"] = final_dat["connections_array"].apply(lambda x: [i.replace('--','') for i in x if "--" in i])
    final_dat["addresses"] = final_dat["connections_array"].apply(lambda x: [i for i in x if "--" not in i])

    #getting the type of action
    final_dat["type"] = final_dat["clusters"].apply(lambda x: 'new' if len(x) == 0 else ('addition' if len(x) == 1 else 'merge') )

    #trim down
    final_dat_slim = final_dat[["clusters", "addresses", "type"]]


    merges_clusters = final_dat_slim[final_dat_slim["type"] == 'merge'].copy()
    new_clusters = final_dat_slim[final_dat_slim["type"] == 'new'].copy()
    addition_clusters = final_dat_slim[final_dat_slim["type"] == 'addition'].copy()


    ### handling 1 at a time for each type
    # additions
    addition_clusters["new_cluster_id"] = addition_clusters["clusters"].apply(lambda x: x[0])
    # new 
    new_clusters["new_cluster_id"] = np.arange(len(new_clusters)) + max_group_id
    max_group_id = max_group_id + len(new_clusters)
    #merges
    merges_clusters["new_cluster_id"] = np.arange(len(merges_clusters)) + max_group_id
    max_group_id = max_group_id + len(merges_clusters)


    # bring together
    full_table_cluster_changes = pd.concat([addition_clusters, new_clusters, merges_clusters])
    
    ## there is a pointer error in numpy and loading a column that is ints
    ## changing the clusters to a string -- will have to convert back in snowflake
    full_table_cluster_changes["new_cluster_id"] = full_table_cluster_changes["new_cluster_id"].astype(str)

    return full_table_cluster_changes
