{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','tx_type'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,sender);",
  tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_transaction_batch') }}
{% if execute %}
  {% set query_blocks %}
  CREATE
  OR REPLACE temporary TABLE silver.transactions__block_intermediate_tmp AS

  SELECT
    DATA,
    partition_key,
    _inserted_timestamp
  FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks_tx') }}
{% else %}
  {{ ref('bronze__streamline_FR_blocks_tx') }}
{% endif %}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(_inserted_timestamp))
    FROM
      {{ this }})
    {% endif %}

    {% endset %}
    {% do run_query(
      query_blocks
    ) %}
    {% set query_tx %}
    CREATE
    OR REPLACE temporary TABLE silver.transactions__tx_intermediate_tmp AS
    SELECT
      DATA,
      partition_key,
      _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
  {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(_inserted_timestamp))
    FROM
      {{ this }})
    {% endif %}

    {% endset %}
    {% do run_query(
      query_tx
    ) %}
    {% set query_tx_batch %}
    CREATE
    OR REPLACE temporary TABLE silver.transactions_tx_batch_intermediate_tmp AS
    SELECT
      DATA,
      VALUE,
      partition_key,
      _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_batch') }}
{% else %}
  {{ ref('bronze__streamline_FR_transaction_batch') }}
{% endif %}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(_inserted_timestamp))
    FROM
      {{ this }})
    {% endif %}

    {% endset %}
    {% do run_query(
      query_tx_batch
    ) %}
    {% endif %}

    WITH from_blocks AS (
      SELECT
        partition_key AS block_number,
        TO_TIMESTAMP(
          DATA :block_timestamp :: STRING
        ) AS block_timestamp,
        b.value :hash :: STRING AS tx_hash,
        b.value :version :: INT AS version,
        b.value :success :: BOOLEAN AS success,
        b.value :type :: STRING AS tx_type,
        b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
        b.value :changes AS changes,
        b.value :epoch :: INT AS epoch,
        b.value :event_root_hash :: STRING AS event_root_hash,
        b.value :events AS events,
        b.value :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
        b.value: failed_proposer_indices AS failed_proposer_indices,
        b.value: gas_unit_price :: bigint AS gas_unit_price,
        b.value :gas_used :: INT AS gas_used,
        b.value :id :: STRING AS id,
        b.value :max_gas_amount :: bigint AS max_gas_amount,
        b.value :payload AS payload,
        b.value :payload :function :: STRING AS payload_function,
        b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec,
        b.value :proposer :: STRING AS proposer,
        b.value :round :: INT AS ROUND,
        b.value :sender :: STRING AS sender,
        b.value :signature :: STRING AS signature,
        b.value :state_change_hash :: STRING AS state_change_hash,
        b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
        b.value :timestamp :: bigint AS TIMESTAMP,
        b.value :vm_status :: STRING AS vm_status,
        {{ dbt_utils.generate_surrogate_key(
          ['tx_hash']
        ) }} AS transactions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
      FROM
        silver.transactions__block_intermediate_tmp A,
        LATERAL FLATTEN (
          DATA :transactions
        ) b
    ),
    from_transactions AS (
      SELECT
        {# b.block_number, #}
        TO_TIMESTAMP(
          DATA :timestamp :: STRING
        ) AS block_timestamp,
        DATA :hash :: STRING AS tx_hash,
        DATA :version :: INT AS version,
        DATA :success :: BOOLEAN AS success,
        DATA :type :: STRING AS tx_type,
        DATA :accumulator_root_hash :: STRING AS accumulator_root_hash,
        DATA :changes AS changes,
        DATA :epoch :: INT AS epoch,
        DATA :event_root_hash :: STRING AS event_root_hash,
        DATA :events AS events,
        DATA :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
        DATA: failed_proposer_indices AS failed_proposer_indices,
        DATA: gas_unit_price :: bigint AS gas_unit_price,
        DATA :gas_used :: INT AS gas_used,
        DATA :id :: STRING AS id,
        DATA :max_gas_amount :: bigint AS max_gas_amount,
        DATA :payload AS payload,
        DATA :payload :function :: STRING AS payload_function,
        DATA :previous_block_votes_bitvec AS previous_block_votes_bitvec,
        DATA :proposer :: STRING AS proposer,
        DATA :round :: INT AS ROUND,
        DATA :sender :: STRING AS sender,
        DATA :signature :: STRING AS signature,
        DATA :state_change_hash :: STRING AS state_change_hash,
        DATA :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
        DATA :timestamp :: bigint AS TIMESTAMP,
        DATA :vm_status :: STRING AS vm_status,
        {{ dbt_utils.generate_surrogate_key(
          ['tx_hash']
        ) }} AS transactions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        A._inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
      FROM
        silver.transactions__tx_intermediate_tmp A
    ),
    from_transaction_batch AS (
      SELECT
        {# b.block_number, #}
        TO_TIMESTAMP(
          b.value :timestamp :: STRING
        ) AS block_timestamp,
        b.value :hash :: STRING AS tx_hash,
        b.value :version :: INT AS version,
        b.value :success :: BOOLEAN AS success,
        b.value :type :: STRING AS tx_type,
        b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
        b.value :changes AS changes,
        b.value :epoch :: INT AS epoch,
        b.value :event_root_hash :: STRING AS event_root_hash,
        b.value :events AS events,
        b.value :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
        b.value: failed_proposer_indices AS failed_proposer_indices,
        b.value: gas_unit_price :: bigint AS gas_unit_price,
        b.value :gas_used :: INT AS gas_used,
        b.value :id :: STRING AS id,
        b.value :max_gas_amount :: bigint AS max_gas_amount,
        b.value :payload AS payload,
        b.value :payload :function :: STRING AS payload_function,
        b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec,
        b.value :proposer :: STRING AS proposer,
        b.value :round :: INT AS ROUND,
        b.value :sender :: STRING AS sender,
        b.value :signature :: STRING AS signature,
        b.value :state_change_hash :: STRING AS state_change_hash,
        b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
        b.value :timestamp :: bigint AS TIMESTAMP,
        b.value :vm_status :: STRING AS vm_status,
        {{ dbt_utils.generate_surrogate_key(
          ['tx_hash']
        ) }} AS transactions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        A._inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
      FROM
        silver.transactions_tx_batch_intermediate_tmp A,
        LATERAL FLATTEN(
          A.data
        ) b
    ),
    tx_combo AS (
      SELECT
        *
      FROM
        from_transactions
      UNION ALL
      SELECT
        *
      FROM
        from_transaction_batch
    ),
    combo AS (
      SELECT
        *
      FROM
        from_blocks
      UNION ALL
      SELECT
        b.block_number,
        A.*
      FROM
        tx_combo A
        JOIN {{ ref('silver__blocks') }}
        b
        ON A.version BETWEEN b.first_version
        AND b.last_version
    )
    SELECT
      *
    FROM
      combo qualify(ROW_NUMBER() over (PARTITION BY tx_hash
    ORDER BY
      _inserted_timestamp DESC)) = 1
