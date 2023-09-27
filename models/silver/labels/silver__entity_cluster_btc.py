import pandas as pd
import networkx as nx
import numpy as np
import datetime

def model(dbt, session):
    
    dbt.config(materialized = "incremental",
                packages = ["networkx"])

    model_dat_sess = session.sql(""" 
    with base as 
    (
    select distinct tx_id, lower(PUBKEY_SCRIPT_ADDRESS) as input_address, count(distinct PUBKEY_SCRIPT_ADDRESS) over (partition by tx_id) as max_index
    from bitcoin.core.fact_inputs 
    where tx_id in (select distinct tx_id from bitcoin.core.fact_inputs 
                    where lower(PUBKEY_SCRIPT_ADDRESS) in (select distinct lower(address) from crosschain.core.dim_labels
                                                    where blockchain = 'bitcoin' and label_type != 'nft'
                                                    )
                    )
        and block_timestamp > current_date - 1460
    ),
    base2 as (
        select * from base where max_index > 1
    )
    select tx_id, 
    array_agg(input_address) WITHIN GROUP (ORDER BY tx_id desc) :: string as ADDRESS_ARRAY
    from base2
    group by tx_id""")

    btc_dat = model_dat_sess.to_pandas()

    ## STRIP AND REMOVE THE FIRST IN THE GROUP
    btc_dat["ADDRESS_ARRAY"] = btc_dat["ADDRESS_ARRAY"].str.strip('[]').str.split(',')
    btc_dat["FIRST"] = btc_dat["ADDRESS_ARRAY"].apply(lambda x: x.pop(0))


    btc_dat_model = btc_dat[["FIRST","ADDRESS_ARRAY"]]


    ### EXPLODE OUT DATA AND DO NETWORK MAP
    btc_dat_e = btc_dat.explode('ADDRESS_ARRAY')
    G = nx.from_pandas_edgelist(btc_dat_e, 'FIRST','ADDRESS_ARRAY')

    edges = [i for i in nx.connected_components(G)]

    ## BIG IS WIDE DATA
    final_dat_big = pd.DataFrame(edges)

    ## THIS IS STRING DATA
    output_list = list(map(str, edges))
    final_dat = pd.DataFrame(output_list, columns = ['connections'])


    ## TAKE WIDE AND pivot it long
    final_dat_long = pd.DataFrame()
    for i in range(final_dat_big.shape[0]):
        temp = final_dat_big.iloc[i]
        temp_df = pd.DataFrame(temp)
        temp_df["group"] = i + 1
        temp_df.columns = ["address", "group"]
        temp_df = temp_df[temp_df["address"].notna()]
        final_dat_long = pd.concat([final_dat_long, temp_df])


    final_dat_long["address"] = final_dat_long['address'].apply(lambda x: x.replace('"', ''))

    ## GET OUR LABELS AND MERGE THEM IN 
    labs_dat_sess = session.sql(""" 
    select distinct lower(address) as address, project_name
    from crosschain.core.dim_labels
    where blockchain = 'bitcoin' and label_type != 'nft'
    """)

    labs_dat = labs_dat_sess.to_pandas()
    labs_dat.columns = ['address', 'project_name']

    final_dat_merged_long = final_dat_long.merge(labs_dat, how = 'left', on = 'address')


    ### APPLY LABELS TO ALL ADDYS IN GROUP
    for group in final_dat_merged_long["group"].unique():
        project_namez = final_dat_merged_long[final_dat_merged_long["group"] == group]["project_name"].unique()
        project_namez = project_namez[~pd.isnull(project_namez)]
        final_dat_merged_long.loc[final_dat_merged_long["group"] == group,"project_name"] = project_namez[0]


    ### add timestamp, fix columns
    final_dat_merged_long["_inserted_timestamp"] = datetime.datetime.today()
    final_dat_merged_long.columns = ["ADDRESS", "ADDRESS_GROUP", "PROJECT_NAME", "_INSERTED_TIMESTAMP"]
    final_dat_merged_long = final_dat_merged_long.reset_index(drop = True)

    ### fix the date
    def fix_date_cols(df, tz='UTC'):
        cols = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in cols:
            df[col] = df[col].dt.tz_localize(tz)

    fix_date_cols(final_dat_merged_long)

    return final_dat_merged_long