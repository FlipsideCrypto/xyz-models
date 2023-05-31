{{ config(
  materialized = 'incremental',
  unique_key = ["chain_id", "block_id"],
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
) }}
-- depends_on: {{ ref('bronze__streamline_tendermint_blocks') }}

SELECT
  block_number AS block_id,
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
      DATA [0] :result :block :data :txs,
      DATA :result :block :data :txs
    )
  ) AS tx_count,
  COALESCE(
    DATA [0] :result :block :header :proposer_address :: STRING,
    DATA :result :block :header :proposer_address :: STRING
  ) AS proposer_address,
  COALESCE(
    DATA [0] :result :block :header :validators_hash :: STRING,
    DATA :result :block :header :validators_hash :: STRING
  ) AS validator_hash,
  _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tendermint_blocks') }}
{% else %}
  {{ ref('bronze__streamline_FR_tendermint_blocks') }}
{% endif %}
WHERE
  DATA [0] :error IS NULL
  AND DATA :error IS NULL
  AND (
    DATA :result :block :header :chain_id :: STRING IS NOT NULL
    OR DATA [0] :result :block :header :chain_id :: STRING IS NOT NULL
  )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) _inserted_timestamp
  FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
  _inserted_timestamp DESC)) = 1
