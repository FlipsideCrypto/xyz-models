{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "score_date::date",
    full_refresh = false,
    version = 1
) }}

{% set current_date_query %}
  SELECT date(sysdate()) as current_date
{% endset %}

{% set results = run_query(current_date_query) %}

{% if execute %}
  {% set current_date_var = var('current_date_var', results.columns[0].values()[0]) %}
{% endif %}

WITH cex_addresses AS (
    SELECT ADDRESS, LABEL_TYPE, label_subtype, label
    FROM {{ source('aptos_gold_core','dim_labels') }} 
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
  txns.sender AS user_address,
  COALESCE(contr.n_contracts, 0) AS n_contracts, -- using COALESCE to handle NULLs from the LEFT JOIN
  COUNT(1) AS n_txn,
  DATE(txns.block_timestamp) AS activity_day,
  -- a complex tx is any tx that is NOT a simple APT transfer; i.e., has input data!
  SUM(CASE WHEN payload:type_arguments <> '[]' THEN 1 ELSE 0 END) AS n_complex_txn
FROM
  {{ source('aptos_gold_core','fact_transactions') }}  txns
  LEFT JOIN (
    SELECT
      sender,
      DATE(block_timestamp) AS activity_day,
      COUNT(DISTINCT value) AS n_contracts
    FROM
      {{ source('aptos_gold_core','fact_transactions')}},
      LATERAL FLATTEN(input => payload:type_arguments)
    WHERE
      block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
      AND LENGTH(value) > 3
    GROUP BY
      sender, activity_day
  ) contr ON contr.sender = txns.sender AND contr.activity_day = DATE(txns.block_timestamp)
WHERE
  txns.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND txns.sender NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND txns.sender NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
GROUP BY
  user_address, n_contracts, DATE(txns.block_timestamp)
),

-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT
    CASE WHEN direction = 'inbound' THEN RECEIVER ELSE sender END AS user_address,
    DATE(block_timestamp) AS activity_day,
    count(distinct(platform)) as n_contracts,
    sum( case when direction = 'inbound' then 1 else 0 end) as n_bridge_in
    FROM
    {{ source('aptos_gold_defi', 'fact_bridge_activity') }} 
    WHERE
    block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND
      user_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
      AND
      user_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
    GROUP BY
user_address, activity_day
),
-- tx from bridge
from_bridge AS (
SELECT
user_address,
sum(n_bridge_in) AS n_bridge_in
FROM
from_bridge_daily
GROUP BY 
user_address
),

-- count withdrawals from centralized exchanges daily
from_cex_daily AS (
    select 
    xf_to.account_address as user_address,
    DATE(xf_from.block_timestamp) AS activity_day,
    COUNT(*) AS n_cex_withdrawals
  from {{ source('aptos_gold_core','fact_transfers') }} xf_to
  join {{ source('aptos_gold_core','fact_transfers') }} xf_from
  on xf_to.tx_hash = xf_from.tx_hash
  and xf_to.event_index - 1 = xf_from.event_index
  and xf_to.amount = xf_from.amount
  and xf_to.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and xf_from.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and xf_to.transfer_event = 'DepositEvent'
  and xf_from.transfer_event = 'WithdrawEvent'
  and xf_from.token_address = '0x1::aptos_coin::AptosCoin'
  and xf_to.token_address = '0x1::aptos_coin::AptosCoin'
  AND xf_from.account_address in (select address from cex_addresses where label_subtype = 'hot_wallet')
  and user_address NOT IN (select address from cex_addresses)
GROUP BY user_address, activity_day
),
-- total cex
from_cex AS (
SELECT
user_address,
sum(n_cex_withdrawals) AS n_cex_withdrawals
FROM
from_cex_daily
GROUP BY 
user_address
),
-- count days across the three sources
combined_days AS (
    SELECT user_address, activity_day FROM activity
    UNION
    SELECT user_address, activity_day FROM from_bridge_daily
    UNION
    SELECT user_address, activity_day FROM from_cex_daily
),
-- count days across the 3 sources
user_activity_summary AS (
    SELECT 
        user_address,
        COUNT(DISTINCT activity_day) AS n_days_active
    FROM combined_days
    GROUP BY user_address
),
-- count txn across the 3 sources
complex_transactions_and_contracts AS (
    SELECT 
        user_address,
        sum(n_txn) AS n_txn,
        SUM(n_complex_txn) AS n_complex_txn,
        SUM(n_contracts) AS n_contracts
    FROM (
        SELECT user_address, n_txn, n_complex_txn, n_contracts FROM activity
        UNION ALL
        SELECT user_address, sum(n_bridge_in) AS n_txn, sum(n_bridge_in) AS n_complex_txn, sum(n_contracts) AS n_contracts FROM from_bridge_daily GROUP BY user_address
        UNION ALL
        SELECT user_address, sum(n_cex_withdrawals) AS n_txn, 0 AS n_complex_txn, 0 AS n_contracts FROM from_cex_daily GROUP BY user_address
    ) AS sub
    GROUP BY user_address
),



-- net token accumulate
xfer_in AS (
    SELECT account_address AS user_address, 
        COUNT(*) AS n_xfer_in
    from {{ source('aptos_gold_core','fact_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND transfer_event = 'DepositEvent'
    AND account_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
    AND account_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
    GROUP BY account_address
), 

xfer_out AS (
    SELECT account_address AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('aptos_gold_core','fact_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND transfer_event = 'WithdrawEvent'
    AND account_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
    AND account_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
    GROUP BY account_address
),

net_token_accumulate AS (
    SELECT
        a.user_address,
        COALESCE((a.n_xfer_in) / (a.n_xfer_in + b.n_xfer_out), 0) AS net_token_accumulate
    FROM xfer_in a
    FULL OUTER JOIN xfer_out b ON a.user_address = b.user_address
),


-- nfts user_address, n_nft_collections, n_nft_trades
nft_trades_long AS (
SELECT
  seller_address AS user_address,
  nft_address AS nft_collection,
  count(tx_hash) AS n_trades
  FROM
  {{ source('aptos_gold_nft','ez_nft_sales') }}
  WHERE
  block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
  seller_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  seller_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
  GROUP BY seller_address, nft_address

UNION

  SELECT
  buyer_address AS user_address,
  nft_address AS nft_collection,
  count(tx_hash) AS n_trades
  FROM
  {{ source('aptos_gold_nft','ez_nft_sales') }}
  WHERE
  block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  GROUP BY buyer_address, nft_address
),

nft_trades AS (
SELECT 
user_address,
sum(n_trades) as n_nft_trades,
count(distinct(nft_collection)) AS n_nft_collections
FROM nft_trades_long
GROUP BY user_address
),

-- nft mints user_address, n_nft_mints
nft_mints AS (
SELECT
nft_to_address as user_address,
count(1) as n_nft_mints
FROM
aptos.nft.ez_nft_mints
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
  nft_to_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  nft_to_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
GROUP BY 
user_address
),

-- gov_stakes user_address, net_stake_accumulate, n_stake_tx

delegation as (
  select 
  case when event_module = 'delegation_pool' then event_data:delegator_address
       when event_module = 'ditto_staking' then event_data:user end as user_address,
  sum( case when event_module = 'delegation_pool' then event_data:amount_added / pow(10,8)
            when event_module = 'ditto_staking' then event_data:apt_amount_staked / pow(10,8) end) as amount_delegated,
  count(1) as n_delegations
  from {{ source('aptos_gold_core','fact_events') }}  events
  where events.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and ((
    event_resource ilike 'AddStake%' and 
    event_module = 'delegation_pool'
  ) OR (event_resource ilike 'StakeEvent%' and 
  event_module = 'ditto_staking'))
  AND
  user_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  user_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
  group by 1
),

undelegation as (
  select 
  case when event_module = 'delegation_pool' then event_data:delegator_address
       when event_module = 'ditto_staking' then event_data:user end as user_address,
  sum( case when event_module = 'delegation_pool' then event_data:amount_withdrawn / pow(10,8)
            when event_module = 'ditto_staking' then event_data:apt_amount_rcvd / pow(10,8) end) as amount_undelegated,
            count(1) as n_undelegations
  from {{ source('aptos_gold_core','fact_events') }}
  where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and (
    (event_module = 'delegation_pool'
     and event_resource = 'WithdrawStakeEvent') OR 
    (event_module = 'ditto_staking'
     and event_resource = 'InstantUnstakeEvent')
  )
  AND
  user_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  user_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
  group by 1
),

gov_stakes AS (
SELECT coalesce(d.user_address,u.user_address)::varchar as user_address,
coalesce(d.n_delegations,0) as n_stake_tx,
coalesce(d.n_delegations, 0) / (coalesce(d.n_delegations, 0) + coalesce(u.n_undelegations, 0)) AS net_stake_accumulate
FROM delegation d join undelegation u on u.user_address = d.user_address
),

-- gov_votes user_address, n_gov_votes
gov_votes AS (
SELECT
  sender as user_address,
  count(1) as n_gov_votes
  from {{ source('aptos_gold_core','fact_events') }} events 
  join {{ source('aptos_gold_core','fact_transactions') }}  txns using (tx_hash)
  where events.block_timestamp  >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and txns.block_timestamp  >= CAST( '{{ current_date_var }}' AS DATE) - 90
  and event_resource = 'VoteEvent'
  AND
  user_address NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  user_address NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
  group by 1
),


-- defi
-- swaps user_address, n_swap_tx
swap_txn AS (
SELECT 
    tx_hash
  FROM {{ source('aptos_gold_core','fact_events') }} events
  WHERE
   event_resource ilike 'Swap%'
  AND
  block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  GROUP BY tx_hash
),

swaps AS (
  SELECT
  sender AS user_address,
  count(distinct(swap_txn.tx_hash)) AS n_swap_tx
  FROM swap_txn
  JOIN (SELECT tx_hash, sender 
    FROM {{ source('aptos_gold_core','fact_transactions')}} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND
    sender NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
    AND
    sender NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
    ) txns ON swap_txn.tx_hash = txns.tx_hash
  GROUP BY user_address
  ),


-- lp_adds user_address, n_lp_adds
lp_adds AS (
SELECT
    txns.sender AS user_address,
    count(distinct(txns.tx_hash)) AS n_lp_adds
  FROM
    {{ source('aptos_gold_core','fact_events') }} events
    JOIN
        (SELECT tx_hash, sender 
    FROM {{ source('aptos_gold_core','fact_transactions')}} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND
    sender NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
    AND
    sender NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
    AND 
    success = TRUE
    ) txns
    ON events.tx_hash = txns.tx_hash
  WHERE
    events.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND events.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND event_resource ilike any (
     'AddLiquidity%',
     'LiquidityAdd%'
    )
    AND event_module ilike any ('%pool%','%swap%', 'controller')
    GROUP BY txns.sender
),


-- other_defi user_address, n_other_defi
other_defi AS (
SELECT 
    sender AS user_address,
    count(e.tx_hash) AS n_other_defi
    
  FROM {{ source('aptos_gold_core','fact_events') }} e
  JOIN (SELECT * FROM {{ source('aptos_gold_core','fact_transactions')}}  WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90) t ON e.tx_hash = t.tx_hash
  WHERE 
(
lower(event_resource) LIKE '%lendingaddliquidity%'
OR
lower(event_resource) LIKE '%lendingremoveliquidity%'
OR
lower(event_resource) LIKE '%borrowevent%'
OR
lower(event_resource) LIKE '%repayborrowevent%'
OR 
lower(event_resource) LIKE '%supplyevent%'
OR
lower(event_resource) LIKE '%repayevent%'
)
AND
  e.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
  sender NOT IN (SELECT token_address FROM {{ source('aptos_gold_core','dim_tokens') }})
  AND
  sender NOT IN (SELECT address FROM {{ source('aptos_gold_core','dim_labels') }})
  GROUP BY sender
),

-- put it all together!
final_output AS (
    SELECT 
        a.user_address,
        COALESCE(a.n_txn, 0) AS n_txn,
        COALESCE(a.n_complex_txn, 0) AS n_complex_txn,
        COALESCE(a.n_contracts, 0) AS n_contracts,
        COALESCE(b.n_bridge_in, 0) AS n_bridge_in,
        COALESCE(c.n_cex_withdrawals, 0) AS n_cex_withdrawals,
        COALESCE(d.net_token_accumulate, 0) AS net_token_accumulate,
        COALESCE(e.n_swap_tx, 0) AS n_swap_tx,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
        COALESCE(f.n_other_defi, 0) AS n_other_defi,
        COALESCE(h.n_nft_trades, 0) AS n_nft_trades,
        COALESCE(h.n_nft_collections, 0) AS n_nft_collections,
        COALESCE(i.n_nft_mints, 0) AS n_nft_mints,
        COALESCE(j.n_stake_tx, 0) AS n_stake_tx,
        COALESCE(j.net_stake_accumulate, 0) AS net_stake_accumulate,
        COALESCE(k.n_gov_votes, 0) AS n_gov_votes
    FROM complex_transactions_and_contracts a
    LEFT JOIN user_activity_summary aa ON a.user_address = aa.user_address
    LEFT JOIN from_bridge b ON a.user_address = b.user_address
    LEFT JOIN from_cex c ON a.user_address = c.user_address
    LEFT JOIN net_token_accumulate d ON a.user_address = d.user_address
    LEFT JOIN swaps e ON a.user_address = e.user_address
    LEFT JOIN other_defi f ON a.user_address = f.user_address
    LEFT JOIN lp_adds g ON a.user_address = g.user_address
    LEFT JOIN nft_trades h ON a.user_address = h.user_address
    LEFT JOIN nft_mints i ON a.user_address = i.user_address
    LEFT JOIN gov_stakes j ON a.user_address = j.user_address
    LEFT JOIN gov_votes k ON a.user_address = k.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_txn > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 5 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 3 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 4 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 0.07 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 3 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 4 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 2 THEN 1 ELSE 0 END
         + CASE WHEN net_stake_accumulate > 0.4 THEN 1 ELSE 0 END
         + CASE WHEN n_gov_votes > 2 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'aptos'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'aptos' AS blockchain,
        '{{ model.config.version }}' AS score_version,
        user_address,
        CURRENT_TIMESTAMP AS calculation_time,
        CAST( '{{ current_date_var }}' AS DATE) AS score_date,
        activity_score + tokens_score + defi_score + nfts_score + gov_score AS total_score,
        activity_score,
        tokens_score,
        defi_score,
        nfts_score,
        gov_score
    FROM scores
    {% if is_incremental() %}        
        WHERE 
        {% if current_date_var == modules.datetime.datetime.utcnow().date() %}
            score_date > (SELECT MAX(score_date) FROM {{ this }})
        {% else %}
            score_date = CAST('{{ current_date_var }}' AS DATE)
        {% endif %}
    {% endif %}
)

SELECT * FROM total_scores