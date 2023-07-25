{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['BLOCK_NUMBER', 'TX_HASH', 'POSITION']) }} AS input_id,
    *
FROM
    {{ ref('silver__transactions') }}
