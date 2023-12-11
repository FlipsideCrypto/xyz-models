{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BITCOIN, PRICE' }}},
    tags = ['prices', 'core']
) }}

SELECT
    HOUR,
    CLOSE AS price,
    provider,
    COALESCE(
        price_all_providers_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['hour', 'provider']
        ) }}
    ) AS ez_hourly_token_prices_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__price_all_providers_hourly') }}
    qualify ROW_NUMBER() over (
        PARTITION BY HOUR
        ORDER BY
            priority
    ) = 1
