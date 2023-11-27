{{ config (
    materialized = "ephemeral"
) }}

WITH pending_tx AS (

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE = CURRENT_DATE() - 3
        AND is_pending = 'TRUE'
)
SELECT
    block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex,
    tx_hash
FROM
    pending_tx
