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
    SELECT ADDRESS, LABEL_TYPE, project_name AS label
    FROM {{ source('blast_gold_core','dim_labels') }} 
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
        from_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(DISTINCT CASE WHEN input_data != '0x' THEN to_address END) AS n_contracts,
        sum(CASE WHEN input_data != '0x' THEN 1 ELSE 0 END) AS n_complex_txn
    FROM {{ source('blast_gold_core','fact_transactions') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }} )
    AND from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    GROUP BY from_address, activity_day
),

-- curate some bridge transactions
bridge_base as (
select 
to_address as user_address,
block_timestamp,
case when from_address = lower('0x24850c6f61C438823F01B7A3BF2B89B72174Fa9d') then 'wormhole'
        when from_address = lower('0x5e023c31e1d3dcd08a1b3e8c96f6ef8aa8fcacd1') then 'rhinofi'
        when from_address = lower('0xf70da97812cb96acdf810712aa562db8dfa3dbef') then 'relay'
        when from_address = lower('0x80c67432656d59144ceff962e8faf8926599bcf8') then 'orbiter'
end as platform
from {{ source('blast_gold_core','ez_native_transfers') }} 
where from_address in (lower('0x24850c6f61C438823F01B7A3BF2B89B72174Fa9d'),
                        lower('0x5e023c31e1d3dcd08a1b3e8c96f6ef8aa8fcacd1'),
                        lower('0xf70da97812cb96acdf810712aa562db8dfa3dbef'),
                        lower('0x80c67432656d59144ceff962e8faf8926599bcf8'))
and block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90

union all


select to_address as user_address,
block_timestamp,
'native' as platform
from {{ source('blast_gold_core','ez_native_transfers') }}
where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
and from_address = to_address
and origin_from_address = origin_to_address
),

-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_bridge_in,
        COUNT(DISTINCT platform) AS n_contracts
    FROM bridge_base
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND user_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND user_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
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
        to_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('blast_gold_core','ez_native_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    GROUP BY to_address, activity_day
    UNION
    SELECT 
        TO_ADDRESS AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('blast_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND FROM_ADDRESS IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
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
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    GROUP BY TO_ADDRESS
), 

xfer_out AS (
    SELECT FROM_ADDRESS AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('blast_gold_core','ez_token_transfers') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    GROUP BY FROM_ADDRESS
),

net_token_accumulate AS (
    SELECT
        a.user_address,
        COALESCE((a.n_xfer_in) / (a.n_xfer_in + b.n_xfer_out), 0) AS net_token_accumulate
    FROM xfer_in a
    FULL OUTER JOIN xfer_out b ON a.user_address = b.user_address
),

--some nft curations
base_nft_buys as (
-- opensea
select
tx_hash,
decoded_log:consideration[0]:recipient::string as user_address,
decoded_log:consideration[0]:token::string as nft_address
from {{ source('blast_gold_core','ez_decoded_event_logs') }} 
where contract_address = '0x0000000000000068f116a894984e2db1123eb395'
and event_name = 'OrderFulfilled'
and block_timestamp > CAST( '{{ current_date_var }}' AS DATE) - 90
and decoded_log:consideration[0]:amount::float < 1000

UNION ALL
-- element marketplace
select
tx_hash,
origin_from_address as user_address,
coalesce(decoded_log:erc721Token::string,decoded_log:erc1155Token::string)  as nft_address
from {{ source('blast_gold_core','ez_decoded_event_logs') }}
where contract_address = '0x4196b39157659bf0de9ebf6e505648b7889a39ce'
and event_name ilike '%OrderFilled%'
and block_timestamp > CAST( '{{ current_date_var }}' AS DATE) - 90
),

-- nfts user_address, n_nft_collections, n_nft_trades
nft_buys AS (
    SELECT 
        user_address, 
        COUNT(distinct(nft_address)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades
    FROM base_nft_buys
    WHERE user_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND user_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    GROUP BY user_address
),

-- nft mints user_address, n_nft_mints
rawmints as (
select
*,
case when topics[0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' then 'erc-1155'
     when (topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
       and topics[3] IS NOT NULL) then 'erc-721'
     else 'erc-20' end as token_standard
from {{ source('blast_gold_core','ez_decoded_event_logs') }}
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
decoded_log:to NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
AND 
decoded_log:to NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
group by user_address
),

-- governance (just the blast token)
gov_mints AS (
    SELECT
        to_address AS user_address,
        COUNT(distinct(contract_address)) AS n_validators,
        COUNT(tx_hash) AS n_stake_tx
    FROM {{ source('blast_gold_core','ez_token_transfers') }}
    WHERE 
        contract_address IN ('0xb1a5700fA2358173Fe465e6eA4Ff52E36e88E2ad')
    AND BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND to_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND from_address = '0x0000000000000000000000000000000000000000'
    GROUP BY to_address
),

gov_restakes AS (
    SELECT 
        origin_from_address AS user_address,
        COUNT(tx_hash) AS n_restakes
    FROM {{ source('blast_gold_core','ez_decoded_event_logs') }}
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND contract_address IN ('0xb1a5700fA2358173Fe465e6eA4Ff52E36e88E2ad')
    AND event_name IN ('Deposit', 'Submitted', 'DepositedFromStaking')
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    GROUP BY origin_from_address
),
-- defi
-- n_swaps user_address, n_swap_tx
-- lp_adds user_address, n_lp_adds
-- other_defi user_address, n_other_defi

swaps_base AS (
--BLASTER
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND origin_to_address = '0xc972fae6b524e8a6e0af21875675bf58a3133e60'
    and to_address != '0xc972fae6b524e8a6e0af21875675bf58a3133e60'
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    
    UNION ALL
    
-- THRUSTER
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND origin_to_address = '0x337827814155ecbf24d20231fca4444f530c0555'
    and to_address != '0x337827814155ecbf24d20231fca4444f530c0555'
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    
    UNION ALL
    
-- AMBIENT FINANCE
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address = '0xaaaaaaaaffe404ee9433eef0094b6382d81fb958'
    AND tx_hash IN (
        SELECT tx_hash
        FROM {{ source('blast_gold_core','ez_token_transfers') }} 
        WHERE BLOCK_TIMESTAMP > CAST( '{{ current_date_var }}' AS DATE) - 90
        GROUP BY tx_hash
        HAVING COUNT(*) > 1
    )
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})

),
    
    
swaps_in AS (
    SELECT 
        origin_from_address AS user_address, 
        COUNT(*) AS n_swap_tx
    FROM swaps_base
    group by 1
),

lp_base_txs as (
SELECT tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP > CAST( '{{ current_date_var }}' AS DATE) - 90
    GROUP BY tx_hash
    HAVING COUNT(*) = 1
),

lp_base AS (
--BLASTER
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND origin_to_address = '0xc972fae6b524e8a6e0af21875675bf58a3133e60'
    and to_address = '0xc972fae6b524e8a6e0af21875675bf58a3133e60'
    AND tx_hash IN (
        SELECT tx_hash
        FROM lp_base_txs
    )
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    
    UNION ALL
    
-- THRUSTER
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND origin_to_address = '0x337827814155ecbf24d20231fca4444f530c0555'
    and to_address = '0x337827814155ecbf24d20231fca4444f530c0555'
    AND tx_hash IN (
        SELECT tx_hash
        FROM lp_base_txs
    )
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    
    UNION ALL
    
-- AMBIENT FINANCE
    SELECT 
        distinct 
        origin_from_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_token_transfers') }} 
    WHERE BLOCK_TIMESTAMP >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND to_address = '0xaaaaaaaaffe404ee9433eef0094b6382d81fb958'
    AND tx_hash IN (
        SELECT tx_hash
        FROM lp_base_txs
    )
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})

),


lp_adds AS (
    SELECT 
        origin_from_address AS user_address, 
        COUNT(*) AS n_lp_adds
    FROM lp_base
    group by 1
),


-- list of lp and swap transactions to exclude from other defi below
lps_swaps AS (
SELECT 
distinct
tx_hash
FROM
(select * from lp_base
union all
select * from swaps_base)
),


-- other defi is a broad category
other_defi_base AS (
    SELECT
        origin_from_address AS user_address,
        tx_hash
    FROM {{ source('blast_gold_core','ez_decoded_event_logs') }}
    WHERE block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND contract_address NOT IN ('0xb1a5700fA2358173Fe465e6eA4Ff52E36e88E2ad')

    AND tx_hash NOT IN (SELECT tx_hash FROM lps_swaps)
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
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
    
    
    union all
    
    select trader as user_address,
    tx_hash
    from {{ source('blast_gold_blitz','ez_perp_trades') }} 
    where block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
    AND trader NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_contracts') }})
    AND trader NOT IN (SELECT address FROM {{ source('blast_gold_core','dim_labels') }})
    
),


other_defi as (
    SELECT
        user_address,
        COUNT(distinct(tx_hash)) AS n_other_defi
    FROM other_defi_base
    group by 1
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
         + CASE WHEN n_complex_txn > 8 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 3 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 0 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 6 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 4 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 0 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 5 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 0 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 5 THEN 1 ELSE 0 END
         + CASE WHEN n_restakes > 1 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_address', "'blast'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'blast' AS blockchain,
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