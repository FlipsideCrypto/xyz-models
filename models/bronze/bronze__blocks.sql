{{ config(
    materialized = 'incremental',
    unique_key = 'block_height',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["block_height"],
) }}

SELECT 
    block_height, 
    data, 
    _inserted_timestamp 
FROM {{ source('bronze', 'sample_blocks') }}