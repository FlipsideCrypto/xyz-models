{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'tx_id',
    cluster_by = ["tx_id"],
) }}

WITH inputs AS (
    SELECT
        *
    FROM
        {{ ref('silver__inputs_final') }} -- -- cluster_by "block_number", "tx_id"
    WHERE
        block_number >= 813567
        AND block_number <= 813692
    -- {% if is_incremental() %}
    --     _inserted_timestamp >= (
    --         SELECT
    --             MAX(_inserted_timestamp) _inserted_timestamp
    --         FROM
    --             {{ this }}
    -- )
    -- {% endif %}
), 
output AS (
    SELECT
        *
    FROM
        {{ ref('silver__inputs_final') }} -- cluster_by ["_partition_by_block_id", "tx_id"],
    WHERE
        block_number >= 813567
    AND block_number <= 813692
),
full_entity_cluster AS (
    SELECT
        *
    FROM
        {{ ref('silver__full_entity_cluster') }}
),

inputs_mapped AS (
  SELECT
    tx_id,
    COALESCE(project_name, CAST(address_group AS varchar)) AS from_entity
  FROM
        inputs fi
    LEFT JOIN full_entity_cluster ec ON fi.pubkey_script_address = ec.address
  group by
    tx_id,
    from_entity
) 
SELECT
  fo.tx_id,
  block_timestamp,
  from_entity,
  COALESCE(
    project_name,
    CAST(address_group AS varchar),
    PUBKEY_SCRIPT_ADDRESS
  ) AS to_entity,
  sum(value) AS transfer_amount,
  SYSDATE() AS inserted_timestamp,
  SYSDATE()   AS modified_timestamp
FROM
  output fo 
  LEFT JOIN full_entity_cluster ec ON fo.pubkey_script_address = ec.address
  LEFT JOIN inputs_mapped ON inputs_mapped.tx_id = fo.tx_id
GROUP BY
  fo.tx_id,
  block_timestamp,
  from_entity,
  to_entity

  -- 1 day is 17s