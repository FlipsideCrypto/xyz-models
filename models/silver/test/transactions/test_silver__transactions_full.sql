{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__transactions') }}
WHERE tx_id IS NOT NULL 
