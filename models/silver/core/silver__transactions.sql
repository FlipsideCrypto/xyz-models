{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['modified_timestamp::DATE','tx_type'],
  tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__blocks_tx') }}
-- depends_on: {{ ref('bronze__transactions') }}
WITH from_blocks AS (

  SELECT
    TO_TIMESTAMP(
      b.value :timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :version :: INT AS version,
    b.value :type :: STRING AS tx_type,
    b.value AS DATA,
    inserted_timestamp AS file_last_updated
  FROM

{% if is_incremental() %}
{{ ref('bronze__blocks_tx') }}
{% else %}
  {{ ref('bronze__blocks_tx_FR') }}
{% endif %}

A,
LATERAL FLATTEN (DATA :transactions) b

{% if is_incremental() %}
WHERE
  A.inserted_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(modified_timestamp))
    FROM
      {{ this }})
    {% endif %}
  ),
  from_transactions AS (
    SELECT
      TO_TIMESTAMP(
        b.value :timestamp :: STRING
      ) AS block_timestamp,
      b.value :hash :: STRING AS tx_hash,
      b.value :version :: INT AS version,
      b.value :type :: STRING AS tx_type,
      b.value AS DATA,
      inserted_timestamp AS file_last_updated
    FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
{% else %}
  {{ ref('bronze__transactions_FR') }}
{% endif %}

A,
LATERAL FLATTEN(A.data) b

{% if is_incremental() %}
WHERE
  A.inserted_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(modified_timestamp))
    FROM
      {{ this }})
    {% endif %}
  ),
  combo AS (
    SELECT
      block_timestamp,
      tx_hash,
      version,
      tx_type,
      DATA,
      file_last_updated
    FROM
      from_blocks
    UNION ALL
    SELECT
      block_timestamp,
      tx_hash,
      version,
      tx_type,
      DATA,
      file_last_updated
    FROM
      from_transactions A
  )
SELECT
  COALESCE(
    block_timestamp,
    '1970-01-01 00:00:00.000'
  ) AS block_timestamp,
  tx_hash,
  version,
  tx_type,
  DATA,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash']
  ) }} AS transactions_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combo qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
  file_last_updated DESC)) = 1
