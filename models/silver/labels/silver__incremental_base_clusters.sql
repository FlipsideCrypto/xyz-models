{{ config(
  materialized = 'view',
  tags = ['snowflake', 'cluster', 'labels', 'entity_cluster', 'incremental']
) }}

SELECT
  address_group,
  ARRAY_AGG(address) :: STRING AS address_list
FROM
  {{ ref(
    "silver__full_entity_cluster"
  ) }}
WHERE
  address IN (
    SELECT
      pubkey_script_address
    FROM
      {{ ref('silver__inputs_final') }}
    WHERE
    _inserted_timestamp between (
      SELECT
        MAX(_inserted_timestamp)
      FROM
        {{ ref(
          "silver__full_entity_cluster"
        ) }}
    )
    and 
    (
      SELECT
        DATEADD(HOUR, 12, MAX(_inserted_timestamp))
      FROM
        {{ ref(
          "silver__full_entity_cluster"
        ) }}
    )
  )
GROUP BY
  address_group
