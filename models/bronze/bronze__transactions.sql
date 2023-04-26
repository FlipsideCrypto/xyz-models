{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["block_height"],
) }}

SELECT 
    block_height, 
    data, 
    _inserted_timestamp 
FROM 
    {{ source('bronze', 'sample_txs') }}