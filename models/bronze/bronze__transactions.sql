{{ config (
    materialized = 'view'
) }}

SELECT 
    block_height, 
    data :data :result :txs :hash :: STRING as tx_id,
    data, 
    _inserted_timestamp 
FROM 
    {{ source('bronze', 'sample_txs') }}
WHERE
    DATA: error IS NULL 

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1