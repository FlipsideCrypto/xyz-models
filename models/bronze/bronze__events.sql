{{ config (
  materialized = 'view'
) }}

SELECT
  A.block_number,
  A.block_height,
  A.block_timestamp,
  A.hash,
  A.type,
  b.index AS event_index,
  b.value :type :: STRING AS event_type,
  b.value :data AS event_data,
  b.value :guid :: STRING AS event_guid,
  b.value :sequence_number :: bigint AS event_sequence_number,
  b.value AS event,
  _inserted_timestamp
FROM
  {{ ref(
    'bronze__transactions'
  ) }} A,
  LATERAL FLATTEN (
    events
  ) b
