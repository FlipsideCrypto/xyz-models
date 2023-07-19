{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH outputs AS (

    SELECT
        block_timestamp,
        block_number,
        block_hash,
        tx_id,
        INDEX,
        pubkey_script_asm,
        pubkey_script_hex,
        pubkey_script_address,
        pubkey_script_type,
        pubkey_script_desc,
        VALUE,
        output_id
    FROM
        {{ ref('silver__outputs') }}
)
SELECT
    *
FROM
    outputs
