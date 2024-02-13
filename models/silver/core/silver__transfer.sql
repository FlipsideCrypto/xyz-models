{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  unique_key = 'tx_id',
  cluster_by = ["from_entity", "to_entity"],
) }}

WITH inputs AS (

  SELECT
    *
  FROM
    {{ ref('silver__inputs_final') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) _inserted_timestamp
    FROM
      {{ this }}
  )
{% endif %}
),
output AS (
  SELECT
    *
  FROM
    {{ ref('silver__outputs_final') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) _inserted_timestamp
    FROM
      {{ this }}
  )
{% endif %}
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
    COALESCE(
      address_group :VARCHAR,
      pubkey_script_address
    ) AS from_entity,
    IFF(
      address_group IS NOT NULL,
      FLOOR(
        address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_from_entity
  FROM
    inputs fi
    LEFT JOIN full_entity_cluster ec
    ON fi.pubkey_script_address = ec.address
  GROUP BY
    tx_id,
    from_entity,
    _partition_by_address_group
),
FINAL AS (
  SELECT
    fo.tx_id,
    block_timestamp,
    from_entity,
    COALESCE(
      address_group :VARCHAR,
      pubkey_script_address
    ) AS to_entity,
    IFF(
      address_group IS NOT NULL,
      FLOOR(
        address_group,
        -3
      ),
      0
    ) AS _partition_by_address_group_to_entity,
    _partition_by_address_group_from_entity,
    SUM(VALUE) AS transfer_amount
  FROM
    output fo
    LEFT JOIN full_entity_cluster ec
    ON fo.pubkey_script_address = ec.address
    LEFT JOIN inputs_mapped
    ON inputs_mapped.tx_id = fo.tx_id
  GROUP BY
    fo.tx_id,
    block_timestamp,
    from_entity,
    to_entity,
    _partition_by_address_group_to_entity,
    _partition_by_address_group_from_entity
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id', 'from_entity', 'to_entity']
  ) }} AS transfer_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  FINAL
