{{ config(
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref(
        'silver__bridge_layerzero_transfers'
    ) }}
