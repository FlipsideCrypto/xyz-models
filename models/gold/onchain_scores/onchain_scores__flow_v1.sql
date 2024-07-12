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

-- set aside centralized exchange addresses for two later cte's
WITH cex_addresses AS (
    SELECT ADDRESS, LABEL_TYPE, label
    FROM {{ source('flow_gold_core','dim_labels') }}
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
        authorizers[0]::string AS user_address,
        count(distinct t.tx_id) AS n_txn,
        DATE(t.block_timestamp) AS activity_day,
         count( distinct CASE 
            WHEN event_type = 'TokensWithdrawn' THEN NULL
            WHEN event_type = 'TokensDeposited' THEN NULL
            WHEN event_type = 'FeesDeducted' THEN NULL
            ELSE EVENT_CONTRACT END
        ) as n_contracts
    FROM {{ source('flow_gold_core', 'fact_transactions') }}  t
    left join {{ source('flow_gold_core','fact_events') }} B
    on t.tx_id = B.tx_id
    WHERE t.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    and B.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND
      t.tx_succeeded = TRUE
    and b.tx_succeeded = TRUE
    AND user_address NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}} )
    AND user_address NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    GROUP BY user_address, activity_day
),

-- count fact_transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        flow_wallet_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_bridge_in,
        COUNT(DISTINCT bridge) AS n_contracts
    FROM {{ source('flow_gold_defi','ez_bridge_transactions') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND 
    direction = 'inbound'
    AND flow_wallet_address NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND flow_wallet_address NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    GROUP BY flow_wallet_address, activity_day
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
    SELECT 
        recipient AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('flow_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND sender IN (SELECT ADDRESS FROM cex_addresses)
    AND recipient NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND recipient NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
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
complex_fact_transactions_and_contracts AS (
    SELECT 
        user_address,
        SUM(n_txn) AS n_txn,
        SUM(n_contracts) AS n_contracts
    FROM (
        SELECT user_address, n_txn, n_contracts FROM activity
        UNION ALL
        SELECT user_address, 0 AS n_txn, n_contracts FROM from_bridge_daily
        UNION ALL
        SELECT user_address, 0 AS n_txn, 0 AS n_contracts FROM from_cex_daily
    ) AS sub
    GROUP BY user_address
),

-- net token accumulate
xfer_in AS (
    SELECT recipient AS user_address, 
        COUNT(*) AS n_xfer_in
    FROM {{ source('flow_gold_core','ez_token_transfers') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND recipient NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND recipient NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    GROUP BY recipient
), 

xfer_out AS (
    SELECT sender AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('flow_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND sender NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND sender NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    GROUP BY sender
),

net_token_accumulate AS (
    SELECT
        a.user_address,
        COALESCE((a.n_xfer_in) / (a.n_xfer_in + b.n_xfer_out), 0) AS net_token_accumulate
    FROM xfer_in a
    FULL OUTER JOIN xfer_out b ON a.user_address = b.user_address
),
-- nfts user_address, n_nft_collections, n_nft_trades
nft_buys AS (
    SELECT 
        buyer AS user_address, 
        COUNT(distinct(NFT_COLLECTION)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades
    FROM {{ source('flow_gold_nft','ez_nft_sales') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND buyer NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND buyer NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    GROUP BY buyer
),

-- nft mints user_address, n_nft_mints
flow_raw_mints as (
  SELECT
  fe.tx_id, fe.block_timestamp, event_data:to::string AS minter, event_data:id::number AS tokenid
  FROM
  {{ source('flow_gold_core','fact_events') }} fe
  WHERE
  event_contract in (select distinct nft_collection from {{ source('flow_gold_nft','ez_nft_sales') }})
  and tx_succeeded
  AND
  event_type = 'Deposit'
  and tx_id in (select distinct tx_id from
      {{ source('flow_gold_core','fact_events') }}
    where
      BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
      and tx_succeeded
      and event_type like '%Mint%')
  AND
  fe.BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
  ),
  
  
nft_mints AS (
    select 
    minter as user_address,
    count(distinct (tx_id) ) as n_nft_mints
    from flow_raw_mints
    WHERE
        user_address NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
        AND user_address NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    group by user_address
  ),
  
lists_offers AS (
  SELECT
  tx_id
  FROM 
  {{ source('flow_gold_core','fact_events') }} 
    WHERE
  BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND 
  (
  lower(event_type) LIKE '%list%'
  OR
  lower(event_type) LIKE '%offer%'
  )
  AND
  tx_id NOT IN (
    SELECT tx_id 
    FROM {{ source('flow_gold_nft','ez_nft_sales') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  )
),

nft_lists as (
  SELECT
  proposer AS user_address, 
  count(distinct(ft.tx_id)) AS n_nft_lists
  FROM
  {{ source('flow_gold_core', 'fact_transactions') }} ft
  JOIN lists_offers lo ON lo.tx_id = ft.tx_id
  WHERE 
    ft.BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
    user_address NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND user_address NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
  
  GROUP BY proposer
),


gov_deposits as (
  SELECT
    DISTINCT delegator as user_address,
    count(distinct tx_id) as n_stakes,
    count(distinct node_id) as n_stake_nodes,
    sum(amount) as stake_amt
  FROM
    {{ source('flow_gold_gov','ez_staking_actions') }} 
  WHERE
    BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    and action in ('DelegatorTokensCommitted','TokensCommitted')
    and tx_succeeded
    and delegator NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND delegator NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
  GROUP BY
    1
),

gov_withdraws as (
  SELECT
    DISTINCT delegator as user_address,
    count(distinct tx_id) as n_unstakes,
    count(distinct node_id) as n_unstake_nodes,
    sum(amount) as unstake_amt
  FROM
    {{ source('flow_gold_gov','ez_staking_actions') }}
  WHERE
    BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    and action in ('DelegatorUnstakedTokensWithdrawn','UnstakedTokensWithdrawn')
    and tx_succeeded
    and delegator NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    AND delegator NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
  GROUP BY
    1
),

gov_stakes as (

select 
coalesce(d.user_address, w.user_address) as user_address,
coalesce(n_stakes, 0) as n_stake_tx,
coalesce(n_stake_nodes, 0) as n_validators,
coalesce(n_unstakes, 0) as n_unstake_tx,
n_stake_tx / (n_stake_tx + n_unstake_tx) as net_stake_accumulate
from
gov_deposits d
full join gov_withdraws w
on d.user_address = w.user_address
),


-- defi
-- n_swaps user_address, n_swap_tx
-- lp_adds user_address, n_lp_adds
-- other_defi user_address, n_other_defi
swaps_in AS (
    SELECT 
        trader AS user_address, 
        COUNT(*) AS n_swap_tx
    FROM {{ source('flow_gold_defi','ez_swaps') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND trader NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    AND trader NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    GROUP BY trader
),

lp_adds AS (
    SELECT 
        sender AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM {{ source('flow_gold_core','ez_token_transfers') }}
    WHERE recipient IN (SELECT vault_address as contract_address FROM flow.defi.dim_swap_pool_labels)
    AND tx_id NOT IN (SELECT tx_id FROM {{ source('flow_gold_defi','ez_swaps') }} WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90)
    and block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND sender NOT IN (SELECT address FROM {{ source('flow_gold_core','dim_labels') }})
    AND sender NOT IN (SELECT account_address FROM {{ source('flow_gold_core','dim_contract_labels')}})
    GROUP BY sender
),


lists_offers AS (
SELECT
tx_id
FROM 
{{ source('flow_gold_core','fact_events') }} 
WHERE
BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
AND 
(
lower(event_type) LIKE '%borrow%'
OR
lower(event_type) LIKE '%lend%'
OR
lower(event_type) LIKE '%repay%'
OR
lower(event_contract) LIKE '%increment%'
OR
lower(event_contract) LIKE '%flowty%'
OR
lower(event_contract) LIKE '%celer%'
OR
lower(event_contract) LIKE '%metapier%'
)
AND
tx_id NOT IN (
  SELECT tx_id 
  FROM {{ source('flow_gold_nft','ez_nft_sales') }}
  WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
)
AND
tx_id NOT IN (
  SELECT tx_id 
  FROM {{ source('flow_gold_defi','ez_swaps') }}
  WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
)
),

other_defi as (
SELECT
proposer AS user_address, 
count(distinct(ft.tx_id)) AS n_other_defi
FROM
{{ source('flow_gold_core', 'fact_transactions') }} ft
JOIN lists_offers lo ON lo.tx_id = ft.tx_id
WHERE 
ft.BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY proposer
),


-- put it all together!
final_output AS (
    SELECT 
        a.user_address,
        COALESCE(aa.n_days_active, 0) AS n_days_active,
        COALESCE(a.n_txn, 0) AS n_txn,
        COALESCE(a.n_contracts, 0) AS n_contracts,
        COALESCE(b.n_bridge_in, 0) AS n_bridge_in,
        COALESCE(c.n_cex_withdrawals, 0) AS n_cex_withdrawals,
        COALESCE(d.net_token_accumulate, 0) AS net_token_accumulate,
        COALESCE(e.n_swap_tx, 0) AS n_swap_tx,
        COALESCE(f.n_other_defi, 0) AS n_other_defi,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
        COALESCE(h.n_nft_trades, 0) AS n_nft_trades,
        COALESCE(l.n_nft_lists, 0) AS n_nft_lists,
        COALESCE(i.n_nft_mints, 0) AS n_nft_mints,
        COALESCE(j.n_stake_tx, 0) AS n_stake_tx,
        COALESCE(j.n_validators, 0) AS n_validators,
        COALESCE(j.net_stake_accumulate, 0) AS net_stake_accumulate
    FROM complex_fact_transactions_and_contracts a
    LEFT JOIN user_activity_summary aa ON a.user_address = aa.user_address
    LEFT JOIN from_bridge b ON a.user_address = b.user_address
    LEFT JOIN from_cex c ON a.user_address = c.user_address
    LEFT JOIN net_token_accumulate d ON a.user_address = d.user_address
    LEFT JOIN swaps_in e ON a.user_address = e.user_address
    LEFT JOIN other_defi f ON a.user_address = f.user_address
    LEFT JOIN lp_adds g ON a.user_address = g.user_address
    LEFT JOIN nft_buys h ON a.user_address = h.user_address
    LEFT JOIN nft_mints i ON a.user_address = i.user_address
    LEFT JOIN nft_lists l ON a.user_address = i.user_address
    LEFT JOIN gov_stakes j ON a.user_address = j.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_days_active > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_txn > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 0 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 2 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate >= 0.33 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 1 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_lists > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 0 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_stake_accumulate >= 0.17 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'flow'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'flow' AS blockchain,
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