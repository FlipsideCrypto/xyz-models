{{ config(
  materialized = 'incremental',
  unique_key = "tx_hash",
  incremental_strategy = 'merge',
   cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','tx_type']
) }}

SELECT
    block_height AS block_number,
    TO_TIMESTAMP(
      DATA :data :block_timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :type :: STRING AS tx_type,
    -- DATA :data :block_height :: INT AS block_height,
    b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
    b.value :changes AS changes,
    b.value :epoch :: INT AS epoch, -- only type block_metadata_transaction (bmt) not null
    b.value :event_root_hash :: STRING AS event_root_hash,
    b.value :events AS events,
    b.value :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs, -- only type user_transaction (ut) not null
    b.value: failed_proposer_indices AS failed_proposer_indices, -- only bmt
    b.value: gas_unit_price :: bigint AS gas_unit_price, -- only ut
    b.value :gas_used :: INT AS gas_used, --only ut > 0
    b.value :id :: STRING AS id, -- only bmt
    b.value :max_gas_amount :: bigint AS max_gas_amount, -- only ut
    b.value :payload AS payload, -- only ut
    b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec, --only bmt
    b.value :proposer :: STRING AS proposer, --only bmt
    b.value :round :: INT AS ROUND, --only bmt
    b.value :sender :: STRING AS sender, --only ut
    b.value :signature :: STRING AS signature, --only ut
    b.value :state_change_hash :: STRING AS state_change_hash,
    b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash, --only type state_checkpoint_transaction (sch) is not null
    b.value :success :: BOOLEAN AS success,
    b.value :timestamp :: bigint AS TIMESTAMP, -- same as block_timestamp
    b.value :version :: INT AS version,
    b.value :vm_status :: STRING AS vm_status, --same as succeeded
     {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
      'aptos_bronze',
      'lq_blocks_txs'
    ) }} A,
    LATERAL FLATTEN (
      DATA :data :transactions
    ) b

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
  _inserted_timestamp DESC)) = 1

