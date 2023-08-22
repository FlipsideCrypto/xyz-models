{{ config(
  severity = 'error'
) }}
-- enabled in addition to sequence gap test to ensure we have the correct blocks by hash
WITH silver_blocks AS (

  SELECT
    block_number,
    block_number - 1 AS missing_block_number,
    block_timestamp,
    block_hash,
    previous_block_hash,
    LAG(block_hash) over (
      ORDER BY
        block_number ASC
    ) AS prior_hash,
    _partition_by_block_id,
    CURRENT_TIMESTAMP AS _test_timestamp
  FROM
    {{ ref('silver__blocks') }}
  WHERE
    block_timestamp :: DATE < CURRENT_DATE
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> previous_block_hash
