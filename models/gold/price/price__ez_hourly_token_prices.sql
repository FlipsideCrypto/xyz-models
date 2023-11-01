{{ config(
    materialized = 'view',
    tags = ['core', 'prices']
) }}

SELECT
    HOUR,
    CLOSE AS price,
    provider
FROM
    {{ ref('silver__price_all_providers_hourly') }}
    qualify ROW_NUMBER() over (
        PARTITION BY HOUR
        ORDER BY
            priority
    ) = 1
