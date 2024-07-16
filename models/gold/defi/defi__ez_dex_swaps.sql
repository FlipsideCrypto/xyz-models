{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,swapper);",
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    A.platform,
    event_address,
    swapper,
    token_in,
    COALESCE(
        t_in.symbol,
        p_in.symbol
    ) AS symbol_in,
    token_out,
    COALESCE(
        t_out.symbol,
        p_out.symbol
    ) AS symbol_out,
    amount_in_unadj,
    CASE
        WHEN COALESCE(
            t_in.decimals,
            p_in.decimals
        ) IS NOT NULL THEN amount_in_unadj / pow(10, COALESCE(t_in.decimals, p_in.decimals))
    END AS amount_in,
    amount_in * p_in.price AS amount_in_usd,
    amount_out_unadj,
    CASE
        WHEN COALESCE(
            t_out.decimals,
            p_out.decimals
        ) IS NOT NULL THEN amount_out_unadj / pow(10, COALESCE(t_out.decimals, p_out.decimals))
    END AS amount_out,
    amount_out * p_out.price AS amount_out_usd,
    fact_dex_swaps_id AS ez_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('defi__fact_dex_swaps') }} A
    LEFT JOIN {{ ref('core__dim_tokens') }}
    t_in
    ON A.token_in = t_in.token_address
    LEFT JOIN {{ ref('core__dim_tokens') }}
    t_out
    ON A.token_out = t_out.token_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p_in
    ON A.token_in = p_in.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p_in.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p_out
    ON A.token_out = p_out.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p_out.hour

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        COALESCE(
            t_in.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            t_out.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p_in.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p_out.modified_timestamp,
            '2000-01-01'
        )
    ) >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
