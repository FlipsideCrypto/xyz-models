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
    FROM {{ source('sei_gold_core','dim_labels') }} 
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity_base AS (
    select 
    tx_from as user_address,
    count(distinct tx_id) as n_total_tx
    from {{source('sei_gold_core','fact_transactions') }} 
    where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    and tx_succeeded = TRUE
    and fee != '0usei'
    AND
    tx_from NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND
    tx_from NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    group by tx_from
),

complex_tx_base AS (
    select 
    tx_from as user_address,
    tx_id
    from {{source('sei_gold_core','fact_transactions') }} 
    where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
      and tx_succeeded = TRUE
      and fee != '0usei'
      and msgs ILIKE '%execute%'
    AND
    tx_from NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND
    tx_from NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    ),
    
n_contracts AS (
    select c.tx_id, attribute_value as contract_
    from {{source('sei_gold_core','fact_msg_attributes')}} a inner join complex_tx_base c
    on a.tx_id = c.tx_id
    where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    and attribute_key = '_contract_address'
    
    ),
    
complex_tx as (   
    select user_address, 
    count(distinct c.tx_id) as n_complex_tx, 
    count(distinct contract_) as n_contracts 
    from complex_tx_base c left join n_contracts n USING(tx_id)
    group by user_address
),

activity as (
select
        a.user_address,
        coalesce(a.n_total_tx, 0 ) as n_txn,
        coalesce(c.n_contracts, 0) as n_contracts,
        coalesce(c.n_complex_tx, 0) as n_complex_txn,
from activity_base a left join complex_tx c using(user_address)
),

-- count transactions from bridges daily
bridge_receives AS (
select block_timestamp, receiver as user_address, tx_id, currency
from {{source('sei_gold_core','fact_transfers')}} t inner join {{ source('sei_gold_core','dim_labels') }} l
on t.sender = l.address 
where label_type = 'bridge'
  and t.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
),

ibc_receives AS (
select block_timestamp, receiver as user_address, tx_id, currency
from {{source('sei_gold_core','fact_transfers')}} 
where transfer_type = 'IBC_TRANSFER_IN'
  and block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
),

combined AS (
select block_timestamp, user_address, tx_id, currency from bridge_receives 
UNION ALL 
select block_timestamp, user_address, tx_id, currency from ibc_receives  
),


-- tx from bridge
from_bridge AS (
SELECT
user_address,
count(distinct tx_id) AS n_bridge_in,
count(distinct currency) as n_contracts
FROM
combined
GROUP BY 
user_address
),

-- total_cex
from_cex AS (
    SELECT 
        receiver AS user_address,
        COUNT(*) AS n_cex_withdrawals
    FROM {{source('sei_gold_core','fact_transfers')}}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND sender IN (SELECT ADDRESS FROM cex_addresses)
    AND receiver NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND receiver NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY receiver
),

-- count txn across the 3 sources
complex_transactions_and_contracts AS (
    SELECT 
        user_address,
        sum(n_txn) as n_txn,
        SUM(n_complex_txn) AS n_complex_txn,
        SUM(n_contracts) AS n_contracts
    FROM (
        SELECT user_address, n_txn, n_complex_txn, n_contracts FROM activity
        UNION ALL
        SELECT user_address, 0 as n_txn, n_bridge_in AS n_complex_txn, n_contracts FROM from_bridge
        UNION ALL
        SELECT user_address, 0 as n_txn, 0 AS n_complex_txn, 0 AS n_contracts FROM from_cex
    ) AS sub
    GROUP BY user_address
),

-- net token accumulate
xfer_in AS (
    SELECT RECEIVER AS user_address, 
        COUNT(*) AS n_xfer_in
    FROM {{source('sei_gold_core','fact_transfers')}}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND RECEIVER NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND RECEIVER NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY RECEIVER
), 

xfer_out AS (
    SELECT SENDER AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{source('sei_gold_core','fact_transfers')}}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND SENDER NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND SENDER NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY SENDER
),

net_token_accumulate AS (
    SELECT
        a.user_address,
        COALESCE((a.n_xfer_in - b.n_xfer_out), 0) AS net_token_accumulate
    FROM xfer_in a
    FULL OUTER JOIN xfer_out b ON a.user_address = b.user_address
),
-- nfts user_address, n_nft_collections, n_nft_trades
nft_buys AS (
    SELECT 
        buyer_address AS user_address, 
        COUNT(distinct(NFT_ADDRESS)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades,
        count(distinct nft_address, token_id) as n_nft_ids
    FROM {{ source('sei_gold_nft','ez_nft_sales') }}
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND buyer_address NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND buyer_address NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY buyer_address
),


-- defi
-- n_swaps user_address, n_swap_tx
-- lp_adds user_address, n_lp_adds
-- other_defi user_address, n_other_defi
swaps_in AS (
    SELECT 
        swapper AS user_address, 
        count(distinct currency_out) as n_tokens_traded,
        COUNT(*) AS n_swap_tx
    FROM {{ source('sei_gold_defi','fact_dex_swaps') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND swapper NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND swapper NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY swapper
),

lp_adds AS (
    SELECT 
        tx_from AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM {{ source('sei_gold_defi','fact_lp_actions')}} l inner join {{source('sei_gold_core','fact_transactions') }} t USING(tx_id)
    WHERE lp_action = 'add_liquidity'
    AND tx_from NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
    AND tx_from NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
    GROUP BY tx_from
),

caller_stake_history AS (
select tx_id,
  tx_caller_address, delegator_address, validator_address,
  case when tx_caller_address != delegator_address then 'potential liquid stake' 
  else 'caller is delegator' end as caller_check,
  action, 
  case when action = 'undelegate' then -1*DIV0(amount, 1e6) 
       when action = 'delegate' then DIV0(amount, 1e6) 
       else 0 end as sei_delegated
from {{ source('sei_gold_gov','fact_staking') }} 
where action IN ('undelegate', 'delegate')
 and BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
 and tx_succeeded = TRUE
),


-- other defi is a broad category
other_defi AS (
  select 
    tx_caller_address as user_address, 
    count(distinct validator_address) as n_validators,
    sum(case when action = 'delegate' then 1 else 0 end) as n_stake_tx,
    sum(case when action = 'undelegate' then 1 else 0 end) as n_unstake_tx,
    sum(sei_delegated) as net_delegated
  from caller_stake_history 
  -- where caller_check = 'caller is delegator'
  
  WHERE
  user_address NOT IN (SELECT currency FROM {{ source('sei_gold_core','dim_tokens') }})
  AND
  user_address NOT IN (SELECT address FROM {{ source('sei_gold_core','dim_labels') }})
  
  group by tx_caller_address
  having net_delegated >= 0
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
        COALESCE(e.n_tokens_traded, 0) AS n_tokens_traded,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
        
        COALESCE(h.n_nft_trades, 0) AS n_nft_trades,
        COALESCE(h.n_nft_collections, 0) AS n_nft_collections,
        COALESCE(h.n_nft_ids, 0) AS n_nft_ids,
        
        COALESCE(f.net_delegated, 0) AS net_delegated,
        COALESCE(f.n_stake_tx, 0) AS n_stake_tx,
        COALESCE(f.n_validators, 0) AS n_validators
        
    FROM complex_transactions_and_contracts a
    LEFT JOIN from_bridge b ON a.user_address = b.user_address
    LEFT JOIN from_cex c ON a.user_address = c.user_address
    LEFT JOIN net_token_accumulate d ON a.user_address = d.user_address
    
    LEFT JOIN swaps_in e ON a.user_address = e.user_address
    LEFT JOIN other_defi f ON a.user_address = f.user_address
    LEFT JOIN lp_adds g ON a.user_address = g.user_address
    LEFT JOIN nft_buys h ON a.user_address = h.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_txn > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 7 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 0 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 5 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 4 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_swap_tx > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_tokens_traded > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 3 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_trades > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 8 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_ids > 0 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN net_delegated > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_stake_tx > 6 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 0 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'sei'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'sei' AS blockchain,
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