{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'INPUTS' }}},
    tags = ['core']
) }}

WITH inputs AS (

    SELECT
        block_timestamp,
        block_number,
        block_hash,
        tx_id,
        INDEX,
        is_coinbase,
        coinbase,
        script_sig_asm,
        script_sig_hex,
        SEQUENCE,
        spent_block_number,
        spent_tx_id,
        spent_output_index,
        pubkey_script_asm,
        pubkey_script_hex,
        pubkey_script_address,
        pubkey_script_type,
        pubkey_script_desc,
        VALUE,
        VALUE_SATS,
        tx_in_witness,
        input_id,
        input_id AS fact_inputs_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
    FROM
        {{ ref('silver__inputs_final') }}
)
SELECT
    *
FROM
    inputs
