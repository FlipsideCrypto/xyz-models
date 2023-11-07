{{ config(
  materialized = 'view',
  tags = ['snowflake', 'cluster', 'labels', 'entity_cluster', 'incremental']
) }}

WITH base AS (

  SELECT
    DISTINCT tx_id,
    pubkey_script_address AS input_address,
    COUNT(
      DISTINCT pubkey_script_address
    ) over (
      PARTITION BY tx_id
    ) AS max_index
  FROM
    {{ source(
      "bitcoin_core",
      "fact_inputs"
    ) }}
  WHERE
    block_timestamp > (
      SELECT
        MAX(_inserted_timestamp)
      FROM
        {{ ref(
          "silver__full_entity_cluster"
        ) }}
    )
),
base2 AS (
  SELECT
    *
  FROM
    base
  WHERE
    max_index > 1
)
SELECT
  tx_id,
  ARRAY_AGG(input_address) within GROUP (
    ORDER BY
      tx_id DESC
  ) :: STRING AS address_array
FROM
  base2
GROUP BY
  tx_id
