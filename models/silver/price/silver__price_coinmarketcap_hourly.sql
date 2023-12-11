{{ config(
    materialized = 'incremental',
    unique_key = 'recorded_hour',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['recorded_hour ::DATE'],
    tags = ["prices", "core", "scheduled_non_core"]
) }}

WITH prices AS (

    SELECT
        *
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_market_cap'
        ) }}
    WHERE
        id = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    recorded_hour :: timestamp_ntz AS recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    volume,
    market_cap,
    'coinmarketcap' AS provider,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['recorded_hour']
    ) }} AS price_coinmarketcap_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prices
