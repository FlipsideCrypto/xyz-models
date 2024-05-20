import pandas as pd
import snowflake.connector
import datetime
import networkx as nx
from snowflake.connector.pandas_tools import write_pandas
import datetime
import os
from tqdm import tqdm


def generate_safe_table_name(prefix="table_"):
    """
    Generate a timestamped table name for Snowflake.

    Parameters:
    - prefix (str): An optional prefix for the table name.

    Returns:
    - str: A table name that includes a timestamp.
    """
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}{timestamp}"

############################
#     Define Constants     #
############################
BASE_PATH = '/home/ec2-user/clustering/btc'
TABLE_NAME = generate_safe_table_name("common_spend_edges_").upper()

##############################
#     Load DB Connection     #
##############################
username = os.environ.get('SNOWFLAKE_USER')
password = os.environ.get('SNOWFLAKE_PASSWORD')
role = os.environ.get('SNOWFLAKE_ROLE')
warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')

print("Connecting to Snowflake ----")
ctx = snowflake.connector.connect(
    user=username,
    password=password,
    account='vna27887.us-east-1',
    role=role,
    warehouse=warehouse,
    database='BITCOIN',
    schema='CLUSTERING_RUNS'
)

ctx.cursor().execute("ALTER SESSION SET ROWS_PER_RESULTSET=800000000")
print("Done!")

#### BASE QUERY
print('Running Snowflake Base Query Now --')
query = '''
with 
base as (
    select distinct tx_id, PUBKEY_SCRIPT_ADDRESS as input_address, count(distinct PUBKEY_SCRIPT_ADDRESS) over (partition by tx_id) as max_index
    from bitcoin.core.fact_inputs
),
base2 as (
    select * from base where max_index > 1
)
select tx_id, 
array_agg(input_address) WITHIN GROUP (ORDER BY tx_id desc) :: string as ADDRESS_ARRAY
from base2
group by tx_id
'''
btc_dat = ctx.cursor().execute(query)
btc_dat = pd.DataFrame.from_records(iter(btc_dat), columns=[x[0] for x in btc_dat.description])
print('----- Done!')

## STRIP AND REMOVE THE FIRST IN THE GROUP
print('Running Network Mapping Now --')
btc_dat["ADDRESS_ARRAY"] = btc_dat["ADDRESS_ARRAY"].str.strip('[]').str.split(',')
btc_dat["FIRST"] = btc_dat["ADDRESS_ARRAY"].apply(lambda x: x.pop(0))

### EXPLODE OUT DATA AND DO NETWORK MAP
btc_dat_e = btc_dat.explode('ADDRESS_ARRAY')
G = nx.from_pandas_edgelist(btc_dat_e, 'FIRST', 'ADDRESS_ARRAY')
print('----- Done!')

print('Running Finding Edges Now --')
# Using a generator to iterate over connected components to save memory
connected_components = nx.connected_components(G)
# Temporarily store edges in a file instead of holding them in memory
edges_file_path = os.path.join(BASE_PATH, 'edges.txt')
with open(edges_file_path, 'w') as file:
    for component in tqdm(connected_components):
        file.write(','.join(component) + '\n')
print('----- Done!')

##################################################
#     Efficient Data Processing and Upload       #
##################################################

print('Formatting Data and Uploading to Snowflake Now --')
# Process the file in chunks and upload to Snowflake
chunk_size = 200000  # Adjust based on memory capacity and Snowflake limits

# Initialize a global group counter before the loop
group_counter = 1

with open(edges_file_path, 'r') as file:
    with tqdm(desc="Processing and Uploading", unit="chunks") as pbar:
        while True:
            lines = [line for _, line in zip(range(chunk_size), file)]
            if not lines:
                break

            # Convert lines to DataFrame
            data = [line.strip().split(',') for line in lines]
            edge_data = [{'ADDRESS': address, 'GROUP_ID': group_counter + i} 
                         for i, edge in enumerate(data) for address in edge if address]
            df = pd.DataFrame(edge_data)
            df["_INSERTED_TIMESTAMP"] = datetime.datetime.today()

            # Upload chunk to Snowflake
            write_pandas(ctx, df, table_name=TABLE_NAME, overwrite=False, auto_create_table=True)

            # Increment the group counter by the number of groups in this batch
            group_counter += len(data)

            pbar.update(1)  # Update the progress bar by one chunk
print('----- Done!')

print('✅ Done!')
