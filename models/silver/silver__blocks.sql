{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
   block_id,
  COALESCE(
    DATA [0] :result :block :header :time :: TIMESTAMP,
    DATA :result :block :header :time :: TIMESTAMP
  ) AS block_timestamp,
  COALESCE(
    DATA [0] :result :block :header :chain_id :: STRING,
    DATA :result :block :header :chain_id :: STRING
  ) AS chain_id,
  ARRAY_SIZE(
    COALESCE(
      data [0] :result :block :data :txs,
      data :result :block :data :txs
    )
  ) AS tx_count,
  COALESCE(
    data [0] :result :block :header :proposer_address :: STRING,
    data :result :block :header :proposer_address :: STRING
  ) AS proposer_address,
  COALESCE(
    DATA [0] :result :block :header :validators_hash :: STRING,
    DATA :result :block :header :validators_hash :: STRING
  ) AS validator_hash,
  _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM
  {{ ref('bronze__tendermint_blocks') }}
WHERE
  data [0] :error IS NULL
  AND DATA :error IS NULL
  AND (DATA :result :block :header :chain_id :: STRING IS NOT NULL 
  OR DATA[0] :result :block :header :chain_id :: STRING IS NOT NULL)


{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
