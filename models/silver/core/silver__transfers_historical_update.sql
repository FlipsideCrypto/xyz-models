{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"]
) }}

WITH changelog AS (

  SELECT
    *
  FROM
    {{ ref('silver__clusters_changelog') }}
),
transfers AS (
  SELECT
    *
  FROM
    {{ ref('silver__transfers') }}
),
merge_clusters AS (
  SELECT
    VALUE :: INT AS prior_cluster_id,
    FLOOR(
      VALUE :: INT,
      -3
    ) AS _partition_by_address_group,
    new_cluster_id
  FROM
    changelog,
    LATERAL FLATTEN("clusters")
  WHERE
    change_type = 'merge'
),
MERGE AS (
  -- TODO test with subset of transfers in known merge from changelog
  SELECT
    t.tx_id,
    COALESCE(
      C.new_cluster_id,
      t.from_entity
    ) AS from_entity,
    COALESCE(
      c2.new_cluster_id,
      t.to_entity
    ) AS to_entity,
    COALESCE(
      C._partition_by_address_group,
      _partition_by_address_group_from_entity
    ) AS _partition_by_address_group_from_entity,
    COALESCE(
      c2._partition_by_address_group,
      _partition_by_address_group_to_entity
    ) AS _partition_by_address_group_to_entity
  FROM
    transfers t
  WHERE
    _partition_by_address_group_from_entity IN (
      SELECT
        _partition_by_address_group
      FROM
        merge_clusters
    )
    OR _partition_by_address_group_to_entity IN (
      SELECT
        _partition_by_address_group
      FROM
        merge_clusters
    )
    LEFT JOIN merge_clusters C
    ON t.from_entity = C.prior_cluster_id -- TODO may be able to do just 1 join
    LEFT JOIN merge_clusters c2
    ON t.to_entity = c2.prior_cluster_id
),
add_or_new_addresses AS (
  SELECT
    VALUE :: STRING AS address,
    FLOOR(
      cluster_id,
      -3
    ) AS _partition_by_address_group,
    new_cluster_id
  FROM
    clusters_changelog,
    LATERAL FLATTEN ("addresses")
  WHERE
    change_type IN (
      'addition',
      'new'
    )
),
-- addition and new are both unclustered address becoming/joining a cluster
add_or_new AS (
  SELECT
    t.tx_id,
    COALESCE(
      A.new_cluster_id,
      from_entity
    ) AS from_entity,
    COALESCE(
      a2.new_cluster_id,
      to_entity
    ) AS to_entity,
    COALESCE(
      A._partition_by_address_group,
      _partition_by_address_group_from_entity
    ) AS _partition_by_address_group_from_entity,
    COALESCE(
      a2._partition_by_address_group,
      _partition_by_address_group_to_entity
    ) AS _partition_by_address_group_to_entity
  FROM
    transfers t
  WHERE
    _partition_by_address_group_from_entity = 0
    OR _partition_by_address_group_to_entity = 0 -- TODO probably add another where clause on address
    LEFT JOIN add_or_new_addresses A
    ON t.from_entity = A.address
    LEFT JOIN add_or_new_addresses a2
    ON t.to_entity = a2.address
)
SELECT
  *
FROM
  MERGE
UNION ALL
SELECT
  *
FROM
  add_or_new
