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
    array_max([_inserted_timestamp, modified_timestamp]) :: TIMESTAMP_NTZ AS _modified_timestamp
  FROM
    {{ ref('silver__inputs_final') }}
WHERE
    -- see comment below on null address field
    PUBKEY_SCRIPT_TYPE not in ('multisig', 'nonstandard', 'nulldata', 'pubkey')
    AND block_timestamp :: DATE >= sysdate() - interval '14 days'
    AND block_timestamp :: DATE != CURRENT_DATE

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
    array_max([_inserted_timestamp, modified_timestamp]) :: TIMESTAMP_NTZ AS _modified_timestamp
  FROM
    {{ ref('silver__outputs') }}
  WHERE
    -- see comment below on null address field
    PUBKEY_SCRIPT_TYPE not in ('multisig', 'nonstandard', 'nulldata', 'pubkey')
    AND block_timestamp :: DATE >= sysdate() - interval '14 days'
    AND block_timestamp :: DATE != CURRENT_DATE

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
    block_number,
    block_timestamp,
    pubkey_script_address,
    address_group,
    IFF(
      address_group IS NOT NULL,
      FLOOR(
        address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_from_entity,
    i._partition_by_block_id,
    i._inserted_timestamp,
    i._modified_timestamp
  FROM
    inputs i
    LEFT JOIN full_entity_cluster ec
    ON i.pubkey_script_address = ec.address
),
label_outputs AS (
  SELECT
    DISTINCT tx_id,
    pubkey_script_address,
    address_group,
    VALUE,
    IFF(
      address_group IS NOT NULL,
      FLOOR(
        address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_to_entity,
    o._inserted_timestamp,
    o._modified_timestamp
  FROM
    outputs o
    LEFT JOIN full_entity_cluster ec
    ON o.pubkey_script_address = ec.address
),
FINAL AS (
  SELECT
    i.tx_id,
    i.block_number,
    i.block_timestamp,
    COALESCE(
      i.address_group :: VARCHAR,
      i.pubkey_script_address
    ) AS from_entity,
    COALESCE(
      o.address_group :: VARCHAR,
      o.pubkey_script_address
    ) AS to_entity,
    i._partition_by_address_group_from_entity,
    o._partition_by_address_group_to_entity,
    i._partition_by_block_id,
    {# TODO - probably remove the groupby inserted/modified timestamp. Join in after agg #}
    i._inserted_timestamp,
    i._modified_timestamp,
    SUM(VALUE) AS transfer_amount
  FROM
    label_outputs o
    LEFT JOIN label_inputs i
    ON o.tx_id = i.tx_id
  WHERE 
    -- filtering out "refund" UTXO generation events
    from_entity != to_entity
  {# TODO - must check for any unintended consequence of granular groupby (duplication)
      can join in descriptive info like ids and timestamps in a later step, if needed
   #}
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10
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
