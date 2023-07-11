{{ config(
  severity = 'error',
  enabled = False
) }}
-- disabled for now as we are testing block gaps by ordinality
WITH silver_blocks AS (

  SELECT
    block_number,
    block_number - 1 as missing_block_number,
    block_timestamp,
    block_hash,
    previous_block_hash,
    LAG(block_hash) over (
      ORDER BY
        block_number ASC
    ) AS prior_hash,
    _partition_by_block_id,
    current_timestamp as _test_timestamp
  FROM
    {{ ref('silver__blocks') }}
  WHERE
    block_timestamp::date < CURRENT_DATE
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> previous_block_hash
