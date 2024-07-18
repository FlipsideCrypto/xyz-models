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

with axelar_gmp_base as (
SELECT 
call:receipt:logs[0]:address::string as token_contract,
call:address::string AS protocol,
call:contract_address::string AS also_protocol,
call:transaction:from::string AS user_address,
call:chain::string AS source_chain,
call:returnValues:destinationChain::string AS destination_chain,
id AS record_id,
created_at,
CASE
      WHEN POSITION('-' IN id) > 0 THEN '-'
      ELSE '_'
  END AS split_on
FROM
{{ source('axelar_axelscan','fact_gmp') }} 
WHERE
status != 'error'
AND
created_at >= CAST( '{{ current_date_var }}' AS DATE) - 90
),

axelar_gmp as (
SELECT 
    'gmp' as bridge_type,
    CASE
        WHEN split_on = '-' THEN SPLIT_PART(record_id, '-', 1)
        ELSE SPLIT_PART(record_id, '_', 1)
    END AS tx_hash,
    created_at as timestamp,
    user_address,
    source_chain,
    destination_chain,
    token_contract
FROM
axelar_gmp_base
),



axelar_xfers_base as (
SELECT 
send_denom as token_contract,
sender_address AS user_address,
recipient_address,
source_chain,
destination_chain,
id AS record_id,
created_at,
CASE
      WHEN POSITION('-' IN id) > 0 THEN '-'
      ELSE '_'
  END AS split_on
FROM
{{ source('axelar_axelscan','fact_transfers') }} 
WHERE
created_at >= CAST( '{{ current_date_var }}' AS DATE) - 90
),

axelar_xfers as (
SELECT 
    'xfer' as bridge_type,
    CASE
        WHEN split_on = '-' THEN SPLIT_PART(record_id, '-', 1)
        ELSE SPLIT_PART(record_id, '_', 1)
    END AS tx_hash,
    created_at as timestamp,
    user_address,
    source_chain,
    destination_chain,
    token_contract
FROM
axelar_xfers_base
),


axelar_squid as (
SELECT 
    'gmp' as bridge_type,
    tx_hash,
    block_timestamp as timestamp,
    sender as user_address,
    source_chain,
    destination_chain,
    token_address as token_contract
FROM
{{ source('axelar_defi','ez_bridge_squid') }} 
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
and tx_hash not in (select distinct tx_hash from axelar_gmp)
and tx_hash not in (select distinct tx_hash from axelar_xfers)
),


axelar_satellite as (
SELECT 
    'xfer' as bridge_type,
    tx_hash,
    block_timestamp as timestamp,
    sender as user_address,
    source_chain,
    destination_chain,
    token_address as token_contract
FROM
{{ source('axelar_defi','ez_bridge_satellite') }} 
WHERE
block_timestamp >= CAST( '{{ current_date_var }}' AS DATE) - 90
and tx_hash not in (select distinct tx_hash from axelar_gmp)
and tx_hash not in (select distinct tx_hash from axelar_xfers)
and tx_hash not in (select distinct tx_hash from axelar_squid)
),


axelar_base as (
select * from axelar_gmp
union all
select * from axelar_xfers
union all
select * from axelar_squid
union all
select * from axelar_satellite

),


-- put it all together!
final_output AS (
    SELECT 
        user_address,
        count(distinct tx_hash) as n_bridges,
        count(distinct source_chain) as n_from_chains,
        count(distinct destination_chain) as n_to_chains,
        count(distinct token_contract) as n_tokens_bridged,
        SUM(CASE WHEN bridge_type = 'gmp' THEN 1 ELSE 0 END) AS use_gmp,
        SUM(CASE WHEN bridge_type = 'xfer' THEN 1 ELSE 0 END) AS use_xfer,
        SUM(CASE WHEN lower(token_contract) = '0xb5fb4be02232b1bba4dc8f81dc24c26980de9e3c' THEN 1 ELSE 0 END) as use_interchain_token
    FROM axelar_base
    group by 1
),

scores AS (
    SELECT
        user_address,
        0 AS activity_score,
        0 AS tokens_score,
        0 AS defi_score,
        0 AS nfts_score,
        0 AS gov_score,
        (CASE WHEN n_bridges > 2 THEN 2 
              WHEN n_bridges = 2 THEN 1 ELSE 0 END
         + CASE WHEN n_from_chains > 2 THEN 2 
              WHEN n_from_chains = 2 THEN 1 ELSE 0 END
         + CASE WHEN n_to_chains > 2 THEN 2 
              WHEN n_to_chains = 2 THEN 1 ELSE 0 END
         + CASE WHEN n_tokens_bridged > 2 THEN 2 
              WHEN n_tokens_bridged = 2 THEN 1 ELSE 0 END
         + CASE WHEN use_gmp > 0 THEN 1 ELSE 0 END
         + CASE WHEN use_xfer > 0 THEN 1 ELSE 0 END
         + CASE WHEN use_interchain_token > 0 THEN 1 ELSE 0 END) AS total_score
    FROM final_output
),

total_scores AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_address', "'axelar'", "'" ~ current_date_var ~ "'"]) }} AS id,        
        'axelar' AS blockchain,
        '{{ model.config.version }}' AS score_version,  
        user_address,
        CURRENT_TIMESTAMP AS calculation_time,
        CAST( '{{ current_date_var }}' AS DATE) AS score_date,
        total_score,
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