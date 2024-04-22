{{ config(
  materialized = 'view',
  tags = ['entity_cluster_0']
) }}

WITH date_range AS (

  SELECT
    MAX(inserted_timestamp) :: DATE :: timestamp_ntz AS max_inserted_timestamp,
    DATEADD(
      'hours',
      '{{ var("INCREMENTAL_CLUSTER_INTERVAL", 24 ) }}',
      MAX(inserted_timestamp) :: DATE :: timestamp_ntz
    ) AS max_inserted_timestamp_interval
  FROM
    {{ ref("silver__full_entity_cluster") }}
),
base AS (
  SELECT
    DISTINCT tx_id,
    pubkey_script_address AS input_address,
    COUNT(
      DISTINCT pubkey_script_address
    ) over (
      PARTITION BY tx_id
    ) AS max_index
  FROM
    {{ ref('silver__inputs_final') }}

    {% if var(
        'INCREMENTAL_CLUSTER_BACKFILL',
        False
      ) %}
    WHERE
      _inserted_timestamp BETWEEN '{{ var('INCREMENTAL_CLUSTER_BACKFILL_START') }}' :: timestamp_ntz
      AND '{{ var('INCREMENTAL_CLUSTER_BACKFILL_END') }}' :: timestamp_ntz
    {% else %}
    WHERE
      _inserted_timestamp BETWEEN (
        SELECT
          max_inserted_timestamp
        FROM
          date_range
      )
      AND (
        SELECT
          max_inserted_timestamp_interval
        FROM
          date_range
      )
    {% endif %}
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
