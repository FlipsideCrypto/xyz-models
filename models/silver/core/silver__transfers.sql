{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, account_address,token_address);",
  tags = ['core','full_test']
) }}

WITH events AS (

  SELECT
    block_number,
    version,
    success,
    block_timestamp,
    block_timestamp :: DATE AS block_date,
    tx_hash,
    event_index,
    event_resource,
    event_data :amount :: bigint AS amount,
    account_address,
    creation_number,
    _inserted_timestamp
  FROM
    {{ ref(
      'silver__events'
    ) }}
  WHERE
    event_module = 'coin'
    AND event_resource IN (
      'WithdrawEvent',
      'DepositEvent'
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
chnges AS (
  SELECT
    block_timestamp :: DATE AS block_date,
    tx_hash,
    change_index,
    change_data,
    change_data :deposit_events :guid :id :creation_num :: INT AS creation_number_deposit,
    change_data :withdraw_events :guid :id :creation_num :: INT AS creation_number_withdraw,
    address,
    change_resource AS token_address
  FROM
    {{ ref(
      'silver__changes'
    ) }}
  WHERE
    change_module = 'coin'
    AND (
      creation_number_deposit IS NOT NULL
      OR creation_number_withdraw IS NOT NULL
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
chnges_dep AS (
  SELECT
    block_date,
    tx_hash,
    address,
    creation_number_deposit AS creation_number,
    token_address
  FROM
    chnges
  WHERE
    creation_number_deposit IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, creation_number_deposit, address
  ORDER BY
    change_index DESC) = 1)
),
chnges_wth AS (
  SELECT
    block_date,
    tx_hash,
    address,
    creation_number_withdraw AS creation_number,
    token_address
  FROM
    chnges
  WHERE
    creation_number_withdraw IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, creation_number_withdraw, address
  ORDER BY
    change_index DESC) = 1)
)
SELECT
  e.block_number,
  e.block_timestamp,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  e.creation_number,
  e.event_resource AS transfer_event,
  e.account_address,
  e.amount,
  REPLACE(
    REPLACE(
      COALESCE(
        dep.token_address,
        wth.token_address
      ),
      'CoinStore<'
    ),
    '>'
  ) AS token_address,
  {{ dbt_utils.generate_surrogate_key(
    ['e.tx_hash','e.event_index']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  e._inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  events e
  LEFT JOIN chnges_dep dep
  ON e.block_date = dep.block_date
  AND e.tx_hash = dep.tx_hash
  AND e.creation_number = dep.creation_number
  AND e.account_address = dep.address
  AND e.event_resource = 'DepositEvent'
  LEFT JOIN chnges_wth wth
  ON e.block_date = wth.block_date
  AND e.tx_hash = wth.tx_hash
  AND e.creation_number = wth.creation_number
  AND e.account_address = wth.address
  AND e.event_resource = 'WithdrawEvent'
