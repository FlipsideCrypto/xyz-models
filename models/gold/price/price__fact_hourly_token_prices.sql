{{ config(
    materialized = 'view',    
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'BITCOIN, PRICE'
            }
        }
    },
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
