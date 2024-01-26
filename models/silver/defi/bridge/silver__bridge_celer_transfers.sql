{{ config(
    materialized = 'incremental',
    unique_key = "bridge_celer_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        account_address = '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d'
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-10-19'
{% endif %}
) --outbound
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_data,
    'outbound' AS direction,
    account_address AS bridge_address,
    event_module AS FUNCTION,
    event_resource AS event_name,
    'celer' AS platform,
    event_data :to_chain :: INT AS destination_chain_id,
    CASE
        WHEN event_data :to_chain IS NULL THEN 'Aptos'
        WHEN event_data :to_chain :: INT = 12360001 THEN 'Aptos'
        WHEN event_data :to_chain :: INT = 1 THEN 'Ethereum'
        ELSE 'BNB_CHAIN'
    END AS destination_chain,
    event_data :to_addr :: STRING AS destination_chain_receiver,
    12360001 AS source_chain_id,
    'Aptos' AS source_chain,
    event_data :burner :: STRING AS source_chain_sender,
    event_data :coin_id :: STRING AS token_address,
    event_data :amt :: INT AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_celer_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
WHERE
    event_name = 'BurnEvent'
UNION ALL
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_data,
    'inbound' AS direction,
    account_address AS bridge_address,
    event_module AS FUNCTION,
    event_resource AS event_name,
    'celer' AS platform,
    12360001 AS destination_chain_id,
    'Aptos' AS destination_chain,
    event_data :receiver :: STRING AS destination_chain_receiver,
    event_data :ref_chain_id :: INT AS source_chain_id,
    CASE
        WHEN event_data :ref_chain_id IS NULL THEN 'Aptos'
        WHEN event_data :ref_chain_id = 12360001 THEN 'Aptos'
        WHEN event_data :ref_chain_id = 1 THEN 'Ethereum'
        ELSE 'BNB_CHAIN'
    END AS source_chain,
    event_data :depositor :: STRING AS source_chain_sender,
    event_data :coin_id :: STRING AS token_address,
    event_data :amt :: INT AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_celer_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
WHERE
    event_name = 'MintEvent'
