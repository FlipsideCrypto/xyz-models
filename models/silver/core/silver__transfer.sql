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
    AND _partition_by_block_id = 814000
    AND block_number BETWEEN 813567
    AND 813692 
    {# AND tx_id = '69420c6b5d48807535d5b01555df8b455e38cbad89949cdbd0927a67b11cfbb2' #}

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
    AND _partition_by_block_id = 814000
    AND block_number BETWEEN 813567
    AND 813692 
    {# AND tx_id = '69420c6b5d48807535d5b01555df8b455e38cbad89949cdbd0927a67b11cfbb2' #}

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
  example tx
FROM
  this SET
SELECT
  *
FROM
  bitcoin_dev.silver.transfer -- from bitcoin_dev.silver.inputs_final
  -- from bitcoin_dev.silver.outputs
  -- from bitcoin_dev.silver.transactions
WHERE
  tx_id = '69420c6b5d48807535d5b01555df8b455e38cbad89949cdbd0927a67b11cfbb2'
  AND block_number = 813583;

  -- 2 inputs
  -- both w address bc1qkag02t5ptfplscrtec2ltdapk5hh8vmk29vm9j
  -- values 249.01061636 and 0.02465336 = 249.03526972

  -- 3 outputs
  -- index 2 = nulldata with value 0 and address null
  -- values 248.71402963 to bc1qkag02t5ptfplscrtec2ltdapk5hh8vmk29vm9j and 0.32116104 to bc1qjqcr75tqzxduk37wmykd3j68un5up8ln50323s

  -- so in this tx, total input across 2 UTXO was 249.03526972
  -- looks like this was a ransfer of 0.32116104 to bc1qjqcr75tqzxduk37wmykd3j68un5up8ln50323s
  -- and a "refund" UTXO of 248.71402963 back to bc1qkag02t5ptfplscrtec2ltdapk5hh8vmk29vm9j
  
  -- HOWEVER, neither is clustered
SELECT
  *
FROM
  bitcoin_dev.silver.full_entity_cluster
WHERE
  address IN (
    'bc1qkag02t5ptfplscrtec2ltdapk5hh8vmk29vm9j',
    'bc1qjqcr75tqzxduk37wmykd3j68un5up8ln50323s'
  );
#}



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
