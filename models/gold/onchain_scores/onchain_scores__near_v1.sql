-- set aside centralized exchange addresses for two later cte's
WITH cex_addresses AS (
    SELECT ADDRESS, LABEL_TYPE, project_name AS label
    FROM near.CORE.dim_address_labels
    WHERE LABEL_TYPE = 'cex'
),

-- the 3 activity metrics
activity AS (

  SELECT 
    TX_SIGNER AS user_address,
    DATE(block_timestamp) AS activity_day,
    COALESCE(contr.n_contracts, 0) AS n_contracts, -- using COALESCE to handle NULLs from the LEFT JOIN
    count(distinct tx_hash) AS n_txn
    
  FROM near.core.fact_transactions txns
  LEFT JOIN (
    SELECT
      signer_id,
      DATE(block_timestamp) AS activity_day,
      COUNT(DISTINCT receiver_id) AS n_contracts
    FROM
      near.CORE.FACT_actions_events
    WHERE
      block_timestamp >= current_date - 90
    GROUP BY
      signer_id, activity_day
  ) contr ON contr.signer_id = txns.TX_SIGNER AND contr.activity_day = DATE(txns.block_timestamp)
  WHERE 
    block_timestamp >= current_date - 90
    AND
    user_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    AND
    user_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    GROUP BY user_address, activity_day
),

-- count transactions from bridges daily
from_bridge_daily AS (
    SELECT 
        destination_address AS user_address,
        DATE(block_timestamp) AS activity_day,
        COUNT(*) AS n_bridge_in,
        COUNT(DISTINCT platform) AS n_contracts
    FROM near.defi.ez_bridge_activity
    WHERE block_timestamp >= current_date - 90
    AND destination_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND destination_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    GROUP BY destination_address, activity_day
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
    FROM near.CORE.ez_token_transfers
    WHERE block_timestamp >= current_date - 90
    AND from_address IN (SELECT ADDRESS FROM cex_addresses)
    AND to_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND to_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    GROUP BY to_address, activity_day
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
        SUM(n_contracts) AS n_contracts
    FROM (
        SELECT user_address, 0 as n_complex_txn, n_contracts FROM activity
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
    FROM near.core.ez_token_transfers
    WHERE block_timestamp >= current_date - 90
    AND to_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND to_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    GROUP BY TO_ADDRESS
), 

xfer_out AS (
    SELECT FROM_ADDRESS AS user_address,
        COUNT(*) AS n_xfer_out
    FROM near.core.ez_token_transfers
    WHERE block_timestamp >= current_date - 90
    AND from_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND from_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    GROUP BY FROM_ADDRESS
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
        buyer_address AS user_address, 
        COUNT(distinct(NFT_ADDRESS)) AS n_nft_collections,
        COUNT(*) AS n_nft_trades
    FROM near.nft.ez_nft_sales
    WHERE BLOCK_TIMESTAMP >= current_date - 90
    AND buyer_address NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND buyer_address NOT IN (SELECT address FROM near.core.dim_address_labels)
    GROUP BY buyer_address
),


nft_mints AS (
select 
owner_id as user_address,
count(distinct (tx_hash) ) as n_nft_mints
FROM near.nft.fact_nft_mints
WHERE
owner_id NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
AND 
owner_id NOT IN (SELECT address FROM near.core.dim_address_labels)
group by user_address
),


-- governance (just liquid staking on near)

delegation as (
  select 
  signer_id AS user_address,
  COUNT(*) AS n_delegations,
  COUNT(DISTINCT address) AS n_validators,
  from near.gov.fact_staking_actions
  where events.block_timestamp >= current_date - 90
  and action in ('staking', 'deposited')
  AND
  signer_id NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
  AND 
  signer_id NOT IN (SELECT address FROM near.core.dim_address_labels)
  group by 1
),

undelegation as (
  select 
  signer_id AS user_address,
  COUNT(*) AS n_undelegations
  from near.gov.fact_staking_actions
  where events.block_timestamp >= current_date - 90
  and action in ('unstaking', 'withdrawing')
  AND
  signer_id NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
  AND 
  signer_id NOT IN (SELECT address FROM near.core.dim_address_labels)
  group by 1
),

gov_stakes AS (
SELECT coalesce(d.user_address,u.user_address)::varchar as user_address,
coalesce(d.n_validators,0) as n_validators,
coalesce(d.n_delegations,0) as n_stake_tx,
coalesce(d.n_delegations, 0) / (coalesce(d.n_delegations, 0) + coalesce(u.n_undelegations, 0)) AS net_stake_accumulate
FROM delegation d join undelegation u on u.user_address = d.user_address
),
-- defi
-- n_swaps user_address, n_swap_tx
-- lp_adds user_address, n_lp_adds
-- other_defi user_address, n_other_defi
swaps_in AS (
    SELECT 
        trader AS user_address, 
        COUNT(*) AS n_swap_tx
    FROM near.defi.ez_dex_swaps
    WHERE BLOCK_TIMESTAMP >= current_date - 90
    AND trader NOT IN (SELECT address FROM near.core.dim_address_labels)
    AND trader NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    GROUP BY trader
),

lp_adds AS (
    SELECT 
        FROM_ADDRESS AS USER_ADDRESS, 
        COUNT(*) AS n_lp_adds
    FROM near.core.ez_token_transfers
    WHERE TO_ADDRESS IN (SELECT platform FROM near.defi.ez_dex_swaps WHERE block_timestamp >= current_date - 90)
    AND TX_HASH NOT IN (SELECT TX_HASH FROM near.defi.ez_dex_swaps WHERE block_timestamp >= current_date - 90)
    AND FROM_ADDRESS NOT IN (SELECT address FROM near.core.dim_address_labels)
    AND FROM_ADDRESS NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    GROUP BY FROM_ADDRESS
),
-- list of lp and swap transactions to exclude from other defi below
lps_swaps AS (
SELECT 
tx_hash
FROM
near.core.ez_token_transfers
WHERE
to_address IN (SELECT platform FROM near.defi.ez_dex_swaps)
AND
block_timestamp >= current_date - 90
GROUP BY tx_hash
),
-- other defi is a broad category
other_defi AS (
    SELECT
        sender_id AS user_address,
        COUNT(distinct(tx_hash)) AS n_other_defi
    FROM near.defi.ez_lending
    WHERE block_timestamp >= current_date - 90
    AND tx_hash NOT IN (SELECT tx_hash FROM lps_swaps)
    AND sender_id NOT IN (SELECT contract_address FROM NEAR.CORE.DIM_FT_CONTRACT_METADATA)
    AND sender_id NOT IN (SELECT address FROM near.core.dim_address_labels)
    and actions in ('deposit','deposit_to_reserve', 'increase_collateral', 'borrow', 'repay')
    GROUP BY sender_id
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
        COALESCE(j.net_stake_accumulate, 0) AS net_stake_accumulate
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
    LEFT JOIN gov_stakes j ON a.user_address = j.user_address
),

scores AS (
    SELECT
        user_address,
        (CASE WHEN n_days_active > 2 THEN 1 ELSE 0 END
         + CASE WHEN n_complex_txn > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_contracts > 2 THEN 1 ELSE 0 END) AS activity_score,
        (CASE WHEN n_bridge_in > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_cex_withdrawals > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_token_accumulate > 1 THEN 1 ELSE 0 END) AS tokens_score,
        (CASE WHEN n_other_defi > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_swap_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_lp_adds > 0 THEN 1 ELSE 0 END) AS defi_score,
        (CASE WHEN n_nft_mints > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_collections > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_nft_trades > 0 THEN 1 ELSE 0 END) AS nfts_score,
        (CASE WHEN n_stake_tx > 0 THEN 1 ELSE 0 END
         + CASE WHEN n_validators > 0 THEN 1 ELSE 0 END
         + CASE WHEN net_stake_accumulate > 0 THEN 1 ELSE 0 END) AS gov_score
    FROM final_output
),

total_scores AS (
    SELECT 
        'near' AS blockchain,
        user_address,
        CURRENT_TIMESTAMP AS calculation_time,
        CURRENT_DATE AS score_date,
        activity_score + tokens_score + defi_score + nfts_score + gov_score AS total_score,
        activity_score,
        tokens_score,
        defi_score,
        nfts_score,
        gov_score
    FROM scores
)

SELECT * FROM total_scores;