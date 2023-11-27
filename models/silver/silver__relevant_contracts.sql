{{ config (
    materialized = "table",
    unique_key = "contract_address",
    tag = ["core"]
) }}

SELECT
    contract_address,
    'Aurora' AS blockchain,
    COUNT(*) AS events,
    MAX(block_number) AS latest_block
FROM
    {{ ref('silver__logs') }}
GROUP BY
    1,
    2
HAVING
    COUNT(*) >= 25
