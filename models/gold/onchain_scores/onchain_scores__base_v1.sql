{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "score_date::date",
    full_refresh = false,
    version='1'
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
    SELECT ADDRESS, LABEL_TYPE, project_name AS label
    FROM {{ source('base_gold_core','dim_labels') }}
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
        from_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(DISTINCT CASE WHEN input_data != '0x' THEN to_address END) AS n_contracts,
        sum(CASE WHEN input_data != '0x' THEN 1 ELSE 0 END) AS n_complex_txn
    FROM {{ source('base_gold_core','fact_transactions') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY from_address, activity_day
),

-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        receiver AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_bridge_in,
        COUNT(DISTINCT platform) AS n_contracts
    FROM {{ source('base_gold_defi','ez_bridge_activity') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND receiver NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND receiver NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY receiver, activity_day
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
        to_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('base_gold_core','ez_native_transfers') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY to_address, activity_day
    UNION
    SELECT 
        TO_ADDRESS AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('base_gold_core','ez_token_transfers') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND FROM_ADDRESS IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY TO_ADDRESS, activity_day
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
        SUM(n_complex_txn) AS n_complex_txn,
        SUM(n_contracts) AS n_contracts
    FROM (
        SELECT user_address, n_complex_txn, n_contracts FROM activity
        UNION ALL
        SELECT user_address, n_bridge_in AS n_complex_txn, n_contracts FROM from_bridge_daily
        UNION ALL
        SELECT user_address, 0 AS n_complex_txn, 0 AS n_contracts FROM from_cex_daily
    ) AS sub
    GROUP BY user_address
),

-- net token accumulate
xfer_in AS (
    SELECT TO_ADDRESS AS user_address, 
        COUNT(*) AS n_xfer_in
    FROM {{ source('base_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY TO_ADDRESS
), 

xfer_out AS (
    SELECT FROM_ADDRESS AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('base_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY FROM_ADDRESS
),

net_token_accumulate AS (
    SELECT
        a.user_address,
        COALESCE((a.n_xfer_in) / (a.n_xfer_in + b.n_xfer_out), 0) AS net_token_accumulate
    FROM xfer_in a
    FULL OUTER JOIN xfer_out b ON a.user_address = b.user_address
),
-- nfts
nft_buys AS (
    SELECT 
        buyer_address AS user_address, 
        COUNT(distinct(NFT_ADDRESS)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades
    FROM {{ source('base_gold_nft','ez_nft_sales') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND buyer_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND buyer_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    GROUP BY buyer_address
),

rawmints as (
select
*,
case when topics[0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' then 'erc-1155'
     when (topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
       and topics[3] IS NOT NULL) then 'erc-721'
     else 'erc-20' end as token_standard
from {{ source('base_gold_core','ez_decoded_event_logs') }} 
where topics[0] in (
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
  '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
)
AND 
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
AND decoded_log:from = '0x0000000000000000000000000000000000000000'
having token_standard IN ('erc-721','erc-1155')
),

nft_mints AS (
select 
decoded_log:to :: string as user_address,
count(distinct (tx_hash) ) as n_nft_mints
FROM rawmints
WHERE
decoded_log:to NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
AND 
decoded_log:to NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
group by user_address
),
-- governance (just liquid staking on base)
gov_mints AS (
    SELECT
        to_address AS user_address,
        COUNT(distinct(contract_address)) AS n_validators,
        COUNT(tx_hash) AS n_stake_tx
    FROM {{ source('base_gold_core','ez_token_transfers') }}
    WHERE 
        contract_address IN (lower('0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c'),
                     lower('0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22'),
                     lower('0xcf3D55c10DB69f28fD1A75Bd73f3D8A2d9c595ad'),
                     lower('0xD4a0e0b9149BCee3C920d2E00b5dE09138fd8bb7'))
    AND BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    AND to_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND from_address = '0x0000000000000000000000000000000000000000'
    GROUP BY to_address
),

gov_restakes AS (
    SELECT 
        origin_from_address AS user_address,
        COUNT(tx_hash) AS n_restakes
    FROM {{ source('base_gold_core','ez_decoded_event_logs') }}
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND contract_address IN (lower('0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c'),
                     lower('0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22'),
                     lower('0xcf3D55c10DB69f28fD1A75Bd73f3D8A2d9c595ad'),
                     lower('0xD4a0e0b9149BCee3C920d2E00b5dE09138fd8bb7'))
    AND event_name IN ('Deposit', 'Submitted', 'DepositedFromStaking')
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    GROUP BY origin_from_address
),
-- defi
swaps_in AS (
    SELECT 
        origin_from_address AS user_address, 
        COUNT(*) AS n_swap_tx
    FROM {{ source('base_gold_defi','ez_dex_swaps') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    GROUP BY origin_from_address
),

lp_adds AS (
    SELECT 
        FROM_ADDRESS AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM {{ source('base_gold_core','ez_token_transfers') }}
    WHERE TO_ADDRESS IN (SELECT contract_address FROM {{ source('base_gold_defi','ez_dex_swaps') }} WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90)
    AND TX_HASH NOT IN (SELECT TX_HASH FROM {{ source('base_gold_defi','ez_dex_swaps') }} WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90)
    AND FROM_ADDRESS NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    AND FROM_ADDRESS NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    GROUP BY FROM_ADDRESS
),
-- list of lp and swap transactions to exclude from other defi below
lps_swaps AS (
SELECT 
tx_hash
FROM
{{ source('base_gold_core','ez_token_transfers') }}
WHERE
to_address IN (SELECT pool_address FROM base.defi.dim_dex_liquidity_pools)
AND
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
GROUP BY tx_hash
),
-- other defi is a broad category
other_defi AS (
    SELECT
        origin_from_address AS user_address,
        COUNT(distinct(tx_hash)) AS n_other_defi
    FROM {{ source('base_gold_core','ez_decoded_event_logs') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND contract_address NOT IN (lower('0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c'),
                     lower('0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22'),
                     lower('0xcf3D55c10DB69f28fD1A75Bd73f3D8A2d9c595ad'),
                     lower('0xD4a0e0b9149BCee3C920d2E00b5dE09138fd8bb7'))

    AND tx_hash NOT IN (SELECT tx_hash FROM lps_swaps)
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_contracts') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('base_gold_core','dim_labels') }})
    AND (
        lower(event_name) LIKE '%claim%' OR
        lower(event_name) LIKE '%deposit%' OR
        lower(event_name) LIKE '%borrow%' OR
        lower(event_name) LIKE '%lend%' OR
        lower(event_name) LIKE '%supply%' OR
        lower(event_name) LIKE '%liquid%' OR
        lower(event_name) LIKE '%fees%' OR
        lower(event_name) LIKE '%repay%'
    )
    GROUP BY origin_from_address
),
-- put it all together!
final_output AS (
    SELECT 
        a.user_address,
        COALESCE(aa.n_days_active, 0) AS n_days_active,
        COALESCE(a.n_complex_txn, 0) AS n_complex_txn,
        COALESCE(a.n_contracts, 0) AS n_contracts,
        COALESCE(b.n_bridge_in, 0) AS n_bridge_in,
        COALESCE(c.n_cex_withdrawals, 0) AS n_cex_withdrawals,
        COALESCE(d.net_token_accumulate, 0) AS net_token_accumulate,
        COALESCE(e.n_swap_tx, 0) AS n_swap_tx,
        COALESCE(f.n_other_defi, 0) AS n_other_defi,
        COALESCE(g.n_lp_adds, 0) AS n_lp_adds,
        COALESCE(h.n_nft_trades, 0) AS n_nft_trades,
        COALESCE(h.n_nft_collections, 0) AS n_nft_collections,
        COALESCE(i.n_nft_mints, 0) AS n_nft_mints,
        COALESCE(j.n_stake_tx, 0) AS n_stake_tx,
        COALESCE(j.n_validators, 0) AS n_validators,
        COALESCE(k.n_restakes, 0) AS n_restakes
    FROM complex_transactions_and_contracts a
    LEFT JOIN user_activity_summary aa ON a.user_address = aa.user_address
    LEFT JOIN from_bridge b ON a.user_address = b.user_address
    LEFT JOIN from_cex c ON a.user_address = c.user_address
    LEFT JOIN net_token_accumulate d ON a.user_address = d.user_address
    LEFT JOIN swaps_in e ON a.user_address = e.user_address
    LEFT JOIN other_defi f ON a.user_address = f.user_address
    LEFT JOIN lp_adds g ON a.user_address = g.user_address
    LEFT JOIN nft_buys h ON a.user_address = h.user_address
    LEFT JOIN nft_mints i ON a.user_address = i.user_address
    LEFT JOIN gov_mints j ON a.user_address = j.user_address
    LEFT JOIN gov_restakes k ON a.user_address = k.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_days_active > 8 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 3 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 7 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 0 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 7 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 8 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 5 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 7 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_restakes > 0 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['user_address', "'base'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'base' AS blockchain,
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