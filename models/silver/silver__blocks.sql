{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
  block_height :: INTEGER AS block_id, 
  data: data :result :block :header :time :: TIMESTAMP as block_timestamp, 
  data: data :result :block :header :chain_id :: STRING as chain_id,
  array_size(data :data :result :block :data :txs) as tx_count,
  data :data :result :block :header :proposer_address :: STRING as proposer_address, 
  data: data :result :block :header :validators_hash :: STRING AS validator_hash, 
  _inserted_timestamp :: TIMESTAMP as _inserted_timestamp, 
  concat_ws(
    '-',
    chain_id,
    block_id
  ) _unique_key
FROM EVMOS.BRONZE.SAMPLE_BLOCKS -- jinjafy this 

{% if is_incremental() %}
WHERE _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
