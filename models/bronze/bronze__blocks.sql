{{ config (
  materialized = 'view'
) }}

SELECT
  block_height AS block_number,
  DATA :data :block_hash :: STRING AS block_hash,
  DATA :data :block_height :: INT AS block_height,
  DATA :data :block_timestamp :: STRING AS block_timestamp_num,
  TO_TIMESTAMP(block_timestamp_num) AS block_timestamp,
  DATA :data :first_version :: bigint AS first_version,
  DATA :data :last_version :: bigint AS last_version,
  ARRAY_SIZE(
    DATA :data :transactions
  ) AS tx_count_from_transactions_array,
  last_version - first_version + 1 AS tx_count_from_fl_version,
  _inserted_timestamp
FROM
  {{ source(
    'aptos_bronze',
    'lq_blocks_txs'
  ) }}
