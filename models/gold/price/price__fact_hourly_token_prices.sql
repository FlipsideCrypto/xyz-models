{{ config(
    materialized = 'view',
    tags = ['core', 'prices']
) }}

SELECT
    HOUR,
    price,
    provider
FROM
    {{ ref('silver__price_all_providers_hourly') }}
