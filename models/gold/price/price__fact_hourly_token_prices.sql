{{ config(
    materialized = 'view',
    tags = ['prices', 'core']
) }}

SELECT
    hour,
    open,
    high,
    low,
    close,
    provider
FROM
    {{ ref('silver__price_all_providers_hourly') }}
