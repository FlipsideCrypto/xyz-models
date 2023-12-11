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
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
FROM
    {{ ref('silver__price_all_providers_hourly') }}
    qualify ROW_NUMBER() over (
        PARTITION BY HOUR
        ORDER BY
            priority
    ) = 1
