{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'TRANSFERS' }}},
    tags = ['core']
) }}

SELECT
    tx_id,
    block_number,
    block_timestamp,
    from_entity,
    to_entity,
    transfer_amount,
    transfer_id AS fact_clustered_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__transfers') }}
