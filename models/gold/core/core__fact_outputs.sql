{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'OUTPUTS' }}},
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
        VALUE_SATS,
        output_id,
        output_id AS fact_outputs_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
    FROM
        {{ ref('silver__outputs') }}
)
SELECT
    *
FROM
    outputs
