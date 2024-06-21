{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "score_date::date",
    full_refresh = false,
    tags = ['gold', 'onchain_scores', 'avalanche_scores']
) }}

-- set aside centralized exchange addresses for two later cte's
WITH cex_addresses AS (
    SELECT ADDRESS, LABEL_TYPE, project_name AS label
    FROM {{ source('avalanche_gold_core', 'dim_labels') }}
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (
    SELECT
        from_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(DISTINCT CASE WHEN input_data != '0x' THEN to_address END) AS n_contracts,
        sum(CASE WHEN input_data != '0x' THEN 1 ELSE 0 END) AS n_complex_txn
    FROM {{ source('avalanche_gold_core', 'fact_transactions') }}
    WHERE block_timestamp >= current_date - 90
    AND from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY from_address, activity_day
),

-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        receiver AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_bridge_in,
        COUNT(DISTINCT platform) AS n_contracts
    FROM {{ source('avalanche_gold_defi', 'ez_bridge_activity') }}
    WHERE block_timestamp >= current_date - 90
    AND receiver NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND receiver NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY receiver, activity_day
),

-- tx from bridge
from_bridge AS (
    SELECT
        user_address,
        sum(n_bridge_in) AS n_bridge_in
    FROM from_bridge_daily
    GROUP BY user_address
),

-- count withdrawals from centralized exchanges daily
from_cex_daily AS (
    SELECT 
        to_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('avalanche_gold_core', 'ez_native_transfers') }}
    WHERE block_timestamp >= current_date - 90
    AND from_address IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY to_address, activity_day
    UNION
    SELECT 
        TO_ADDRESS AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_cex_withdrawals
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE block_timestamp >= current_date - 90
    AND FROM_ADDRESS IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY TO_ADDRESS, activity_day
),

-- total cex
from_cex AS (
    SELECT
        user_address,
        sum(n_cex_withdrawals) AS n_cex_withdrawals
    FROM from_cex_daily
    GROUP BY user_address
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
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE block_timestamp >= current_date - 90
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY TO_ADDRESS
), 

xfer_out AS (
    SELECT FROM_ADDRESS AS user_address,
        COUNT(*) AS n_xfer_out
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE block_timestamp >= current_date - 90
    AND from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
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
    FROM {{ source('avalanche_gold_nft', 'ez_nft_sales') }}
    WHERE BLOCK_TIMESTAMP >= current_date - 90
    AND buyer_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND buyer_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    GROUP BY buyer_address
),

rawmints as (
    select
        *,
        case when topics[0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' then 'erc-1155'
            when (topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
            and topics[3] IS NOT NULL) then 'erc-721'
            else 'erc-20' end as token_standard
        from {{ source('avalanche_gold_core', 'ez_decoded_event_logs') }}
    where topics[0] in (
        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
        '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
        )
    AND 
        block_timestamp >= current_date - 90
    AND decoded_log:from = '0x0000000000000000000000000000000000000000'
    having token_standard IN ('erc-721','erc-1155')
),

nft_mints AS (
    SELECT 
        decoded_log:to :: string as user_address,
        count(distinct (tx_hash) ) as n_nft_mints
    FROM rawmints
    WHERE
        decoded_log:to NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND 
        decoded_log:to NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    group by user_address
),

-- governance (just liquid staking on avalanche)
gov_mints AS (
    SELECT
        to_address AS user_address,
        COUNT(distinct(contract_address)) AS n_validators,
        COUNT(tx_hash) AS n_stake_tx
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE 
        contract_address IN ('0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be',
                     '0xc3344870d52688874b06d844e0c36cc39fc727f6',
                     '0xa25eaf2906fa1a3a13edac9b9657108af7b703e3',
                     '0xf7d9281e8e363584973f946201b82ba72c965d27',
                     '0x6026a85e11bd895c934af02647e8c7b4ea2d9808')
    AND BLOCK_TIMESTAMP >= current_date - 90
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    AND to_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND from_address = '0x0000000000000000000000000000000000000000'
    GROUP BY to_address
),

gov_restakes AS (
    SELECT 
        origin_from_address AS user_address,
        COUNT(tx_hash) AS n_restakes
    FROM {{ source('avalanche_gold_core', 'ez_decoded_event_logs') }}
    WHERE BLOCK_TIMESTAMP >= current_date - 90
    AND contract_address IN ('0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be',
                     '0xc3344870d52688874b06d844e0c36cc39fc727f6',
                     '0xa25eaf2906fa1a3a13edac9b9657108af7b703e3',
                     '0xf7d9281e8e363584973f946201b82ba72c965d27',
                     '0x6026a85e11bd895c934af02647e8c7b4ea2d9808')
    AND event_name IN ('Deposit', 'Submitted', 'DepositedFromStaking')
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    GROUP BY origin_from_address
),

-- defi
swaps_in AS (
    SELECT 
        origin_from_address AS user_address, 
        COUNT(*) AS n_swap_tx
    FROM {{ source('avalanche_gold_defi', 'ez_dex_swaps') }}
    WHERE BLOCK_TIMESTAMP >= current_date - 90
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    GROUP BY origin_from_address
),

lp_adds AS (
    SELECT 
        FROM_ADDRESS AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE TO_ADDRESS IN (SELECT contract_address FROM {{ source('avalanche_gold_defi', 'ez_dex_swaps') }} WHERE block_timestamp >= current_date - 90)
    AND TX_HASH NOT IN (SELECT TX_HASH FROM {{ source('avalanche_gold_defi', 'ez_dex_swaps') }} WHERE block_timestamp >= current_date - 90)
    AND FROM_ADDRESS NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
    AND FROM_ADDRESS NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    GROUP BY FROM_ADDRESS
),

-- list of lp and swap transactions to exclude from other defi below
lps_swaps AS (
    SELECT 
        tx_hash
    FROM {{ source('avalanche_gold_core', 'ez_token_transfers') }}
    WHERE
        to_address IN (SELECT pool_address FROM {{ source('avalanche_gold_defi', 'dim_dex_liquidity_pools') }})
    AND
        block_timestamp >= current_date - 90
    GROUP BY tx_hash
),

-- other defi is a broad category
other_defi AS (
    SELECT
        origin_from_address AS user_address,
        COUNT(distinct(tx_hash)) AS n_other_defi
    FROM {{ source('avalanche_gold_core', 'ez_decoded_event_logs') }}
    WHERE block_timestamp >= current_date - 90
    AND contract_address NOT IN ('0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be',
                     '0xc3344870d52688874b06d844e0c36cc39fc727f6',
                     '0xa25eaf2906fa1a3a13edac9b9657108af7b703e3',
                     '0xf7d9281e8e363584973f946201b82ba72c965d27',
                     '0x6026a85e11bd895c934af02647e8c7b4ea2d9808')

    AND tx_hash NOT IN (SELECT tx_hash FROM lps_swaps)
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_contracts') }})
    AND origin_from_address NOT IN (SELECT address FROM {{ source('avalanche_gold_core', 'dim_labels') }})
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
        (CASE WHEN n_days_active > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts >= 5 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 3 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 0 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 1 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 0 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 0 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_restakes > 1 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        
        {{ dbt_utils.generate_surrogate_key(['user_address', "'avalanche'",'current_date']) }} AS id,
        'avalanche' AS blockchain,
        user_address,
        CURRENT_TIMESTAMP AS calculation_time,
        current_date AS score_date,
        activity_score + tokens_score + defi_score + nfts_score + gov_score AS total_score,
        activity_score,
        tokens_score,
        defi_score,
        nfts_score,
        gov_score
    FROM scores
    {% if is_incremental() %}
    WHERE score_date > (SELECT MAX(score_date) FROM {{ this }})
    {% endif %}

)

SELECT * FROM total_scores
