{{ config (
  materialized = 'view'
) }}

SELECT
  block_height AS block_number,
  DATA :data :block_height :: INT AS block_height,
  TO_TIMESTAMP(
    DATA :data :block_timestamp :: STRING
  ) AS block_timestamp,
  b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
  b.value :changes AS changes,
  b.value :epoch :: INT AS epoch,
  b.value :event_root_hash :: STRING AS event_root_hash,
  b.value :events AS events,
  b.value: failed_proposer_indices AS failed_proposer_indices,
  b.value :gas_used :: INT AS gas_used,
  b.value :hash :: STRING AS HASH,
  b.value :id :: STRING AS id,
  b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec,
  b.value :proposer :: STRING AS proposer,
  b.value :round :: INT AS ROUND,
  b.value :state_change_hash :: STRING AS state_change_hash,
  b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
  b.value :success :: BOOLEAN AS success,
  b.value :timestamp :: bigint AS TIMESTAMP,
  b.value :type :: STRING AS TYPE,
  b.value :version :: INT AS version,
  b.value :vm_status :: STRING AS vm_status,
  _inserted_timestamp
FROM
  {{ source(
    'aptos_bronze',
    'lq_blocks_txs'
  ) }} A,
  LATERAL FLATTEN (
    DATA :data :transactions
  ) b
