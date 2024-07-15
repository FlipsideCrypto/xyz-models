{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  unique_key = 'transfer_id',
  cluster_by = ["block_timestamp::DATE","_partition_by_address_group_from_entity", "_partition_by_address_group_to_entity", "_modified_timestamp"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,from_entity,to_entity);",
  snowflake_warehouse = "DBT_EMERGENCY"
) }}

WITH inputs AS (

  SELECT
    tx_id,
    block_number,
    block_timestamp,
    pubkey_script_address,
    _partition_by_block_id,
    _inserted_timestamp,
    array_max([inserted_timestamp, modified_timestamp]) :: timestamp_ntz AS _modified_timestamp
  FROM
    {{ ref('silver__inputs_final') }}
  WHERE
    pubkey_script_type NOT IN (
      'multisig',
      'nonstandard',
      'nulldata',
      'pubkey'
    )

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
    VALUE_SATS,
    _partition_by_block_id,
    _inserted_timestamp,
    array_max([inserted_timestamp, modified_timestamp]) :: timestamp_ntz AS _modified_timestamp
  FROM
    {{ ref('silver__outputs') }}
  WHERE
    pubkey_script_type NOT IN (
      'multisig',
      'nonstandard',
      'nulldata',
      'pubkey'
    )

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
    tx_id,
    COALESCE(
      ec.address_group :: VARCHAR,
      i.pubkey_script_address
    ) AS from_entity,
    IFF(
      ec.address_group IS NOT NULL,
      FLOOR(
        ec.address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_from_entity
  FROM
    inputs i
    LEFT JOIN full_entity_cluster ec
    ON i.pubkey_script_address = ec.address
  GROUP BY
    1,
    2,
    3
),
label_outputs AS (
  SELECT
    tx_id,
    pubkey_script_address,
    COALESCE(
      ec.address_group :: VARCHAR,
      o.pubkey_script_address
    ) AS to_entity,
    VALUE,
    VALUE_SATS,
    IFF(
      ec.address_group IS NOT NULL,
      FLOOR(
        ec.address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_to_entity
  FROM
    outputs o
    LEFT JOIN full_entity_cluster ec
    ON o.pubkey_script_address = ec.address
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
),
sum_value AS (
  SELECT
    o.tx_id,
    from_entity,
    to_entity,
    _partition_by_address_group_from_entity,
    _partition_by_address_group_to_entity,
    SUM(VALUE) AS transfer_amount,
    SUM(VALUE_SATS) AS transfer_amount_sats
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
    3,
    4,
    5
),
FINAL AS (
  SELECT
    v.tx_id,
    i.block_number,
    i.block_timestamp,
    v.from_entity,
    v.to_entity,
    v.transfer_amount,
    v.transfer_amount_sats,
    v._partition_by_address_group_from_entity,
    v._partition_by_address_group_to_entity,
    i._partition_by_block_id,
    i._inserted_timestamp,
    i._modified_timestamp
  FROM
    sum_value v
    LEFT JOIN (
      SELECT
        DISTINCT tx_id,
        block_number,
        block_timestamp,
        _partition_by_block_id,
        _inserted_timestamp,
        _modified_timestamp
      FROM
        inputs
    ) i
    ON v.tx_id = i.tx_id
)
SELECT
  tx_id,
  block_number,
  block_timestamp,
  from_entity,
  to_entity,
  transfer_amount,
  transfer_amount_sats,
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
