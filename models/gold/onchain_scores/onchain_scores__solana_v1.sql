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
    FROM {{ source('solana_gold_core','dim_labels') }} 
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
        tx.signers[0]::string AS user_address,
        coalesce(cx.n_contracts, 0) as n_contracts,
        DATE(tx.block_timestamp) AS activity_day,
        count(distinct tx.tx_id) AS n_txn
    FROM {{ source('solana_gold_core','fact_transactions') }} tx
    left join (
        select
        signers[0]::string AS user_address,
        DATE(block_timestamp) AS activity_day,
        count(distinct(program_id)) AS n_contracts
        from SOLANA.CORE.FACT_EVENTS
        where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
        AND user_address NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
        group by user_address, activity_day
    ) cx
    on tx.signers[0]::string = cx.user_address AND cx.activity_day = DATE(tx.block_timestamp)
    WHERE tx.block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND tx.signers[0]::string NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    GROUP BY 1,2,3
),



-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        user_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(distinct tx_id) AS n_bridge_in,
        COUNT(DISTINCT platform) AS n_contracts
    FROM {{ source('solana_gold_defi','fact_bridge_activity') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND user_address NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    and succeeded = TRUE
    and direction = 'inbound'
    GROUP BY user_address, activity_day
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
        tx_to AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(distinct tx_id) AS n_cex_withdrawals
    FROM  {{ source('solana_gold_core','fact_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND tx_from IN (SELECT ADDRESS FROM cex_addresses)
    AND tx_to NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    GROUP BY tx_to, activity_day
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
    SELECT tx_to AS user_address, 
        COUNT(*) AS n_xfer_in
    FROM {{ source('solana_gold_core','fact_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND tx_to NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    GROUP BY tx_to
), 

xfer_out AS (
    SELECT tx_from AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('solana_gold_core','fact_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND tx_from NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    GROUP BY tx_from
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
        purchaser AS user_address, 
        COUNT(distinct(PROGRAM_ID)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades
    FROM {{ source('solana_gold_nft','fact_nft_sales') }}
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND purchaser NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    GROUP BY purchaser
),


nft_mints AS (
    select 
      purchaser as user_address,
      count(*) as n_nft_mints
    FROM {{ source('solana_gold_nft','fact_nft_mints') }}
    WHERE 
      BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
      AND succeeded
      AND mint_currency = 'So11111111111111111111111111111111111111111'
      AND mint_price <= 15
      AND purchaser NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
      and user_address is not null
    GROUP BY purchaser
),


gov_votes AS (
    SELECT
        voter AS user_address
        , COUNT(*) AS n_gov_votes
    FROM {{ source('solana_gold_gov','fact_proposal_votes') }} 
    WHERE 
        BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND voter NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    AND succeeded = TRUE
    GROUP BY voter
),


swaps_in AS (
    SELECT 
        swapper AS user_address
        , COUNT(DISTINCT swap_from_mint) AS n_tokens_traded
        , COUNT(*) AS n_swap_tx
    FROM {{ source('solana_gold_defi','fact_swaps') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND swapper NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
    and succeeded = TRUE
    GROUP BY user_address
),

lp_adds AS (
    SELECT 
        liquidity_provider AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM {{ source('solana_gold_defi','fact_liquidity_pool_actions') }} 
    WHERE
      BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
      AND
      liquidity_provider NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
      and succeeded = TRUE
      and action = 'deposit'
    GROUP BY user_address
),


stake_deposits as (
  SELECT address AS user_address
  , COUNT(*) AS n_stake_tx
  FROM {{ source('solana_gold_defi','fact_stake_pool_actions') }} 
  WHERE 
  BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
  user_address NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
  and succeeded = TRUE
  and action in ('deposit','deposit_stake')
  GROUP BY user_address
  ),
  
stake_withdraws as (
    SELECT address AS user_address
  , COUNT(*) AS n_unstake_tx
  FROM {{ source('solana_gold_defi','fact_stake_pool_actions') }}
  WHERE 
  BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND
  user_address NOT IN (SELECT address FROM {{ source('solana_gold_core','dim_labels') }})
  and succeeded = TRUE
  and action in ('withdraw', 'withdraw_stake')
  GROUP BY user_address
  ),

other_defi as (
      SELECT 
      COALESCE(d.user_address, w.user_address) AS user_address
    , COALESCE(n_stake_tx, 0) as n_stake_tx
    , COALESCE(n_stake_tx, 0) / (COALESCE(n_stake_tx, 0) + COALESCE(n_unstake_tx, 0))  as net_stake_accumulate
    FROM stake_deposits d 
    FULL JOIN stake_withdraws w
        ON d.user_address = w.user_address
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
        COALESCE(e.n_tokens_traded, 0) AS n_tokens_traded,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
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
    LEFT JOIN swaps_in e ON a.user_address = e.user_address
    LEFT JOIN lp_adds g ON a.user_address = g.user_address
    LEFT JOIN nft_buys h ON a.user_address = h.user_address
    LEFT JOIN nft_mints i ON a.user_address = i.user_address
    LEFT JOIN other_defi j ON a.user_address = j.user_address
    LEFT JOIN gov_votes k ON a.user_address = k.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_days_active > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_txn > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 2 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 3 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 0 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_tokens_traded > 5 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 3 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 0 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 1 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_stake_accumulate > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_gov_votes > 1 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'solana'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'solana' AS blockchain,
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