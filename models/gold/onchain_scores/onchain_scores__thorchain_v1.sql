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
    SELECT address, label_type, label as label
    FROM {{ source('thorchain_gold_core','dim_labels') }} 
    WHERE label_type = 'cex'
),


all_tx AS (
SELECT 
date_trunc('day', block_timestamp) AS tx_day,
from_address AS user_address,
'swap' AS tx_type,
count(distinct(tx_id)) AS n_txn
FROM {{ source('thorchain_gold_defi','fact_swaps_events') }}
WHERE from_address NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day

UNION

SELECT
date_trunc('day', block_timestamp) AS tx_day,
from_address AS user_address,
'transfer' AS tx_type,
count(*) AS n_txn
FROM {{ source('thorchain_gold_core','fact_transfers') }}
WHERE from_address NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day

UNION

SELECT
date_trunc('day', block_timestamp) AS tx_day,
to_address AS user_address,
'transfer' AS tx_type,
count(*) AS n_txn
FROM {{ source('thorchain_gold_core','fact_transfers') }}
WHERE to_address NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day

UNION

SELECT
date_trunc('day', block_timestamp) AS tx_day,
from_address AS user_address,
'lp' AS tx_type,
count(*) AS n_txn
FROM  {{ source('thorchain_gold_defi','fact_liquidity_actions') }}
WHERE from_address NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day

UNION

SELECT
date_trunc('day', block_timestamp) AS tx_day,
owner AS user_address,
'lend' AS tx_type,
count(*) AS n_txn
FROM {{source('thorchain_gold_defi','fact_loan_open_events') }} 
WHERE owner NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day

UNION

SELECT
date_trunc('day', block_timestamp) AS tx_day,
owner AS user_address,
'repay' AS tx_type,
count(*) AS n_txn
FROM {{ source('thorchain_gold_defi','fact_loan_repayment_events') }} 
WHERE owner NOT IN (SELECT address FROM {{ source('thorchain_gold_core','dim_labels') }})
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY user_address, tx_day
),


activity_data as (
SELECT
user_address,
sum(n_txn) AS n_txn,
count(distinct(tx_day)) AS n_days_active,
sum(case when tx_type != 'transfer' then n_txn else 0 end) AS n_complex_txn
FROM all_tx
where user_address not in (select distinct address FROM {{ source('thorchain_gold_core','dim_labels') }})
GROUP BY user_address
),


from_cex as (
SELECT 
to_address AS user_address,
COUNT(*) as n_withdrawals
FROM 
{{ source('thorchain_gold_core','fact_transfers') }}   
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
AND
from_address IN (SELECT address FROM cex_addresses) 
AND
to_address NOT IN (SELECT address FROM cex_addresses)
and to_address not in (select distinct address FROM {{ source('thorchain_gold_core','dim_labels') }})
GROUP BY 
user_address
),


swap_data AS (
  SELECT
    *,
    LOWER(
      CASE 
        WHEN from_asset = 'THOR.RUNE' THEN 'thorchain' 
        ELSE SUBSTR(from_asset, 1, POSITION(from_asset, '/') - 1) 
      END
    ) AS from_chain,
    LOWER(
      CASE 
        WHEN to_asset = 'THOR.RUNE' THEN 'thorchain' 
        ELSE SUBSTR(to_asset, 1, POSITION(to_asset, '/') - 1) 
      END
    ) AS to_chain
  FROM
    {{ source('thorchain_gold_defi','fact_swaps_events') }}
  WHERE
    block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
),

swap_chains_assets AS (
  SELECT 
    tx_id, 
    from_address, 
    from_asset AS asset, 
    from_chain AS chain
  FROM 
    swap_data
  UNION ALL
  SELECT 
    tx_id, 
    from_address, 
    to_asset AS asset, 
    to_chain AS chain
  FROM 
    swap_data
),

xchain_swaps AS (
  SELECT
    tx_id,
    from_address,
    COUNT(DISTINCT chain) AS n_chains
  FROM 
    swap_chains_assets
  GROUP BY
    tx_id, from_address
),

multi_chain_swaps AS (
  SELECT 
    from_address, 
    COUNT(*) AS n_multi_chain
  FROM 
    xchain_swaps
  WHERE 
    n_chains > 2
  GROUP BY 
    from_address
),


total_chains AS (
  SELECT 
    from_address, 
    COUNT(DISTINCT chain) AS n_chains
  FROM 
    swap_chains_assets
  GROUP BY 
    from_address
),


affiliate_swaps AS (
  SELECT 
    from_address, 
    COUNT(DISTINCT tx_id) AS n_affiliate_swaps
  FROM 
    swap_data
  WHERE 
    LOWER(memo) ilike LOWER('%THOR-PREFERRED-ASSET-%')
  GROUP BY 
    from_address
),


swap_metrics AS (
  SELECT
    from_address,
    COUNT(DISTINCT tx_id) AS n_swaps,
    COUNT(DISTINCT asset) AS n_tokens_traded
  FROM 
    swap_chains_assets
  GROUP BY 
    from_address
),

dex_swaps as (
SELECT
  sm.from_address AS user_address,
  sm.n_swaps,
  sm.n_tokens_traded,
  COALESCE(asw.n_affiliate_swaps, 0) AS n_affiliate_swaps,
  COALESCE(tc.n_chains, 0) AS n_chains,
  COALESCE(mc.n_multi_chain, 0) AS n_multi_chain
FROM 
  swap_metrics sm
LEFT JOIN 
  affiliate_swaps asw 
ON 
  sm.from_address = asw.from_address
LEFT JOIN 
  total_chains tc 
ON 
  sm.from_address = tc.from_address
LEFT JOIN 
  multi_chain_swaps mc 
ON 
  sm.from_address = mc.from_address
where sm.from_address not in (select distinct address FROM {{ source('thorchain_gold_core','dim_labels') }})
),

lp_data AS ( 
  SELECT 
  from_address AS user_address, 
  count(distinct pool_name) as n_lp_pools,
  count(*) AS n_lp_adds
  
  FROM  {{ source('thorchain_gold_defi','fact_liquidity_actions') }}
  
  WHERE 
  lp_action = 'add_liquidity'
  AND block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
  AND from_address IS NOT NULL
  and from_address NOT IN (SELECT address FROM ethereum.core.dim_labels)
  and from_address not in (select distinct address FROM {{ source('thorchain_gold_core','dim_labels') }})
  GROUP BY user_address
), 


borrows_repays AS (
SELECT
owner AS user_address,
count(*) AS n_other_defi
FROM 
{{source('thorchain_gold_defi','fact_loan_open_events') }}
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY owner

UNION

SELECT
owner AS user_address,
count(*) AS n_other_defi
FROM 
{{ source('thorchain_gold_defi','fact_loan_repayment_events') }}
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY owner
),

other_defi as (
SELECT
user_address,
sum(n_other_defi) AS n_other_defi
FROM
borrows_repays
where user_address not in (select distinct address FROM {{ source('thorchain_gold_core','dim_labels') }})
GROUP BY user_address
),



-- put it all together!
final_output AS (
    SELECT 
        a.user_address,
        COALESCE(a.n_days_active, 0) AS n_days_active,
        COALESCE(a.n_txn, 0) AS n_txn,
        COALESCE(a.n_complex_txn, 0) AS n_complex_txn,
        
        COALESCE(c.n_withdrawals, 0) AS n_withdrawals,
        COALESCE(e.n_tokens_traded, 0) AS n_tokens_traded,
        COALESCE(g.n_lp_pools, 0) AS n_lp_pools,
        
        COALESCE(j.n_other_defi, 0) AS n_other_defi,
        COALESCE(e.n_swaps, 0) AS n_swaps,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
        
        COALESCE(e.n_affiliate_swaps, 0) AS n_affiliate_swaps,
        COALESCE(e.n_chains, 0) AS n_chains,
        COALESCE(e.n_multi_chain, 0) AS n_multi_chain,

    FROM activity_data a
    LEFT JOIN from_cex c ON a.user_address = c.user_address
    LEFT JOIN dex_swaps e ON a.user_address = e.user_address
    LEFT JOIN lp_data g ON a.user_address = g.user_address
    LEFT JOIN other_defi j ON a.user_address = j.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_txn > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_days_active > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 1 THEN 1 ELSE 0 END) AS activity_score,
         
        (CASE WHEN n_withdrawals > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_tokens_traded > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_pools > 1 THEN 1 ELSE 0 END) AS tokens_score,
         
        (CASE WHEN n_other_defi > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_swaps > 3 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 1 THEN 1 ELSE 0 END) AS defi_score,
         
        0 AS nfts_score,
         
        (CASE WHEN n_affiliate_swaps > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_chains > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_multi_chain > 0 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'thorchain'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'thorchain' AS blockchain,
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