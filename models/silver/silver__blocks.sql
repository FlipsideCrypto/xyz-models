{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
  block_height :: INTEGER AS block_id, 
  COALESCE(
    data: data[0] :result :block :header :time :: TIMESTAMP, 
    data: data :result :block :header :time :: TIMESTAMP
  ) as block_timestamp, 
  COALESCE(
    data: data[0] :result :block :header :chain_id :: STRING, 
    data: data :result :block :header :chain_id :: STRING
  ) as chain_id,
  ARRAY_SIZE(
    COALESCE(
      data :data[0] :result :block :data :txs, 
      data :data :result :block :data :txs
  )) as tx_count,
  COALESCE(
    data :data[0] :result :block :header :proposer_address :: STRING, 
    data :data :result :block :header :proposer_address :: STRING
  ) as proposer_address, 
  COALESCE(
    data: data[0] :result :block :header :validators_hash :: STRING, 
    data: data :result :block :header :validators_hash :: STRING  
  ) AS validator_hash, 
  _inserted_timestamp :: TIMESTAMP as _inserted_timestamp, 
  concat_ws(
    '-',
    chain_id,
    block_id
  ) _unique_key
FROM 
  {{ ref('bronze__blocks') }}
WHERE 
    data :data[0] :error IS NULL
    AND data: data :error IS NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
