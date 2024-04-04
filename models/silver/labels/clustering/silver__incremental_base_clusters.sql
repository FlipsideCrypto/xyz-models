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
)
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

    {% if var(
        'TEMP_RUN_STORE',
        False
      ) %}
    WHERE
      tx_id IN (
        SELECT
          DISTINCT tx_id
        FROM
          {{ ref("silver__inputs_final") }}
        WHERE
          block_timestamp >= '2023-10-27' 
          AND pubkey_script_address IN (
            SELECT
              DISTINCT VALUE :: STRING AS address
            FROM
              bitcoin_dev.silver.clusters_changelog_store,
              LATERAL FLATTEN(
                input => addresses
              ) f
            WHERE
              change_type = 'merge'
          )
      )
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
  )
GROUP BY
  address_group
