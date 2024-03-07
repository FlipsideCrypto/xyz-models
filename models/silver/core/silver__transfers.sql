{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  unique_key = 'transfer_id',
  cluster_by = ["_partition_by_address_group_from_entity", "_partition_by_address_group_to_entity", "_modified_timestamp"],
) }}

WITH inputs AS (

  SELECT
    tx_id,
    block_number,
    block_timestamp,
    pubkey_script_address,
    _partition_by_block_id,
    _inserted_timestamp,
    array_max([inserted_timestamp, modified_timestamp]) :: TIMESTAMP_NTZ AS _modified_timestamp
  FROM
    {{ ref('silver__inputs_final') }}
WHERE
    -- see comment below on null address field
    PUBKEY_SCRIPT_TYPE not in ('multisig', 'nonstandard', 'nulldata', 'pubkey')

{% if is_incremental() %}
AND _modified_timestamp >= (
  SELECT
    MAX(_modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
outputs AS (
  SELECT
    tx_id,
    block_number,
    block_timestamp,
    pubkey_script_address,
    VALUE,
    _partition_by_block_id,
    _inserted_timestamp,
    array_max([inserted_timestamp, modified_timestamp]) :: TIMESTAMP_NTZ AS _modified_timestamp
  FROM
    {{ ref('silver__outputs') }}
  WHERE
    -- see comment below on null address field
    PUBKEY_SCRIPT_TYPE not in ('multisig', 'nonstandard', 'nulldata', 'pubkey')

{% if is_incremental() %}
AND _modified_timestamp >= (
  SELECT
    MAX(_modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
full_entity_cluster AS (
  SELECT
    address,
    address_group
  FROM
    {{ ref('silver__full_entity_cluster') }}
),
label_inputs AS (
  SELECT
    DISTINCT tx_id,
    COALESCE(
      ec.address_group :: VARCHAR,
      i.pubkey_script_address
    ) AS from_entity
  FROM
    inputs i
    LEFT JOIN full_entity_cluster ec
    ON i.pubkey_script_address = ec.address
),
label_outputs AS (
  SELECT
    DISTINCT tx_id,
    pubkey_script_address,
    COALESCE(
      ec.address_group :: VARCHAR,
      o.pubkey_script_address
    ) AS to_entity,
    VALUE
  FROM
    outputs o
    LEFT JOIN full_entity_cluster ec
    ON o.pubkey_script_address = ec.address
),
SUM_VALUE AS (
  SELECT
    i.tx_id,
    from_entity,
    to_entity,
    SUM(VALUE) AS transfer_amount
  FROM
    label_outputs o
    LEFT JOIN label_inputs i
    ON o.tx_id = i.tx_id
  WHERE 
    -- filtering out "refund" UTXO generation events
    from_entity != to_entity
  GROUP BY
    1,
    2,
    3
),
FINAL AS (
  {# TODO - join in block num, block ts, ins/mod ts, partitions #}
)
SELECT
  tx_id,
  block_number,
  block_timestamp,
  from_entity,
  to_entity,
  transfer_amount,
  _partition_by_address_group_from_entity,
  _partition_by_address_group_to_entity,
  _partition_by_block_id,
  _inserted_timestamp,
  _modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id', 'from_entity', 'to_entity']
  ) }} AS transfer_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  FINAL


{# 
  TODO - what to do with NULL ADDRESS IN OUTPUTS
  (pubkey is known / need to look in to - AN-4019)
  Note that many of these DO HAVE VALUE associated with them, so should not be overlooked.

  select 
      pubkey_script_type,
      pubkey_script_address is null,
      value > 0,
      count(1) 
  from bitcoin_dev.silver.outputs
  group by 1,2,3
  order by 1,2,3;

 #}
