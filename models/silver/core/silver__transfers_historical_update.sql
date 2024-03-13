{{ config(
  materialized = 'table'
) }}

WITH changelog AS (

  SELECT
    CLUSTERS,
    addresses,
    change_type,
    new_cluster_id
  FROM
    {{ ref('silver__clusters_changelog') }}
),
transfers AS (
  SELECT
    transfer_id,
    tx_id,
    from_entity,
    to_entity,
    _partition_by_address_group_from_entity,
    _partition_by_address_group_to_entity,
    _partition_by_block_id
  FROM
    {{ ref('silver__transfers') }}
),
merge_clusters AS (
  -- multiple existing clusters merging into a new cluster
  -- xfer change: update all old clusters to new id
  SELECT
    VALUE :: STRING AS prior_cluster_id,
    FLOOR(
      VALUE :: INT,
      -3
    ) AS _partition_by_address_group,
    new_cluster_id :: STRING AS new_cluster_id,
    FLOOR(
      new_cluster_id :: INT,
      -3
    ) AS _partition_by_address_group_new,
  FROM
    changelog,
    LATERAL FLATTEN(CLUSTERS)
  WHERE
    change_type = 'merge'
),
update_merge AS (
  SELECT
    t.tx_id,
    CASE
      WHEN C.new_cluster_id IS NOT NULL
        AND c2.new_cluster_id IS NOT NULL THEN 'both'
      WHEN C.new_cluster_id IS NOT NULL THEN 'from'
      WHEN c2.new_cluster_id IS NOT NULL THEN 'to'
      ELSE 'neither'
    END AS record_updated,
    'merge' AS change_type,
    COALESCE(
      C.new_cluster_id,
      t.from_entity
    ) AS from_entity,
    from_entity AS from_entity_prior,
    COALESCE(
      c2.new_cluster_id,
      t.to_entity
    ) AS to_entity,
    to_entity AS to_entity_prior,
    COALESCE(
      C._partition_by_address_group_new,
      _partition_by_address_group_from_entity
    ) AS _partition_by_address_group_from_entity,
    COALESCE(
      c2._partition_by_address_group_new,
      _partition_by_address_group_to_entity
    ) AS _partition_by_address_group_to_entity
  FROM
    transfers t
    LEFT JOIN merge_clusters C
    ON t.from_entity = C.prior_cluster_id
    LEFT JOIN merge_clusters c2
    ON t.to_entity = c2.prior_cluster_id
  WHERE
    _partition_by_address_group_from_entity IN (
      SELECT
        DISTINCT _partition_by_address_group
      FROM
        merge_clusters
    )
    OR _partition_by_address_group_to_entity IN (
      SELECT
        DISTINCT _partition_by_address_group
      FROM
        merge_clusters
    )
),
add_or_new_clusters AS (
  -- previously unclustered addresses becoming/joining a cluster
  -- xfer change: update to_/from_entity FROM an address TO new cluster id
  SELECT
    VALUE :: STRING AS address,
    new_cluster_id,
    FLOOR(
      new_cluster_id,
      -3
    ) AS _partition_by_address_group,
    change_type
  FROM
    changelog,
    LATERAL FLATTEN (addresses)
  WHERE
    change_type IN (
      'addition',
      'new'
    )
),
update_add_or_new AS (
  SELECT
    t.tx_id,
    CASE
      WHEN C.new_cluster_id IS NOT NULL
      AND c2.new_cluster_id IS NOT NULL THEN 'both'
      WHEN C.new_cluster_id IS NOT NULL THEN 'from'
      WHEN c2.new_cluster_id IS NOT NULL THEN 'to'
      ELSE 'neither'
    END AS record_updated,
    COALESCE(
      C.change_type,
      c2.change_type
    ) :: STRING AS change_type,
    COALESCE(
      C.new_cluster_id,
      from_entity
    ) :: STRING AS from_entity,
    from_entity AS from_entity_prior,
    COALESCE(
      c2.new_cluster_id,
      to_entity
    ) AS to_entity,
    to_entity AS to_entity_prior,
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
    LEFT JOIN add_or_new_clusters C
    ON t.from_entity = C.address
    LEFT JOIN add_or_new_clusters c2
    ON t.to_entity = c2.address
  WHERE
    (
      _partition_by_address_group_from_entity = 0
      OR _partition_by_address_group_to_entity = 0
    )
    AND (
      from_entity IN (
        SELECT
          DISTINCT address
        FROM
          add_or_new_clusters
      )
      OR to_entity IN (
        SELECT
          DISTINCT address
        FROM
          add_or_new_clusters
      )
    )
)
SELECT
  *
FROM
  update_merge
UNION ALL
SELECT
  *
FROM
  update_add_or_new


{# 

with
clusters as (
    select value::string as cluster_id from bitcoin.silver.clusters_changelog_store, lateral flatten(input => clusters)
    where inserted_timestamp::date = '2024-03-08'
)
select
    count(1),
    count(distinct tx_id)
from bitcoin_dev.silver.transfers
where 
    to_entity in (select * from clusters)
or
    from_entity in (select * from clusters);

-- prod 3/8
-- 273,585,857	167,078,456

-- prod 3/9
-- 230,698,582	145,836,917

-- prod 3/10
-- 227,957,900	144,921,078

-- prod 3/11
-- 222,547,375	141,812,223 

#}
