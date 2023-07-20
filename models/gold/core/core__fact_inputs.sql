{{ config(
    materialized = 'view',
        meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'INPUTS'
            }
        }
    },
    tags = ['core']
) }}

WITH inputs AS (

    SELECT
        block_timestamp,
        block_number,
        block_hash,
        tx_id,
        index,
        is_coinbase,
        coinbase,
        script_sig_asm,
        script_sig_hex,
        sequence,
        spent_block_number,
        spent_tx_id,
        spent_output_index,
        pubkey_script_asm,
        pubkey_script_hex,
        pubkey_script_address,
        pubkey_script_type,
        pubkey_script_desc,
        value,
        tx_in_witness,
        input_id
    FROM
        {{ ref('silver__inputs_final') }}
)
SELECT
    *
FROM
    inputs
