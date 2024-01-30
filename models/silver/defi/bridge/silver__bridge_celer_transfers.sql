{{ config(
    materialized = 'incremental',
    unique_key = "bridge_celer_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH evnts AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        event_address,
        event_resource,
        event_data,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        event_address = '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d'
        AND event_module IN ('peg_bridge')
        AND event_resource IN ('BurnEvent', 'MintEvent')

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
),
txs AS (
    SELECT
        *
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        payload_function IN (
            '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::peg_bridge::mint',
            '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::peg_bridge::burn',
            '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::peg_bridge::register_mint'
        )

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
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'celer_cbridge' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    'outbound' AS direction,
    b.sender AS tx_sender,
    A.event_data :burner :: STRING AS sender,
    event_data :to_addr :: STRING AS receiver,
    12360001 AS source_chain_id,
    'Aptos' AS source_chain,
    event_data :to_chain :: INT AS destination_chain_id,
    CASE
        WHEN event_data :to_chain IS NULL THEN 'Aptos'
        WHEN event_data :to_chain :: INT = 12360001 THEN 'Aptos'
        WHEN event_data :to_chain :: INT = 1 THEN 'Ethereum'
        WHEN event_data :to_chain :: INT = 56 THEN 'BSC'
        WHEN left(event_data :to_chain,2) = '56' THEN 'BSC'
    END AS destination_chain_name,
    event_data :coin_id :: STRING AS token_address,
    event_data :amt :: INT AS amount_unadj,
     {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash']
    ) }} AS bridge_celer_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    a._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    LEFT JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
WHERE
    event_name = 'BurnEvent'
UNION ALL
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'celer_cbridge' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    'inbound' AS direction,
    b.sender AS tx_sender,
    A.event_data :depositor :: STRING AS sender,
    event_data :receiver :: STRING AS receiver,
    event_data :ref_chain_id :: INT AS source_chain_id,
    CASE
        WHEN event_data :ref_chain_id IS NULL THEN 'Aptos'
        WHEN event_data :ref_chain_id :: INT = 12360001 THEN 'Aptos'
        WHEN event_data :ref_chain_id :: INT = 1 THEN 'Ethereum'
        WHEN event_data :ref_chain_id :: INT = 56 THEN 'BSC'
        WHEN left(event_data :ref_chain_id,2) = '56' THEN 'BSC'
        
    END AS source_chain_id,
    12360001 AS destination_chain_id,
    'Aptos' AS destination_chain_name,
    event_data :coin_id :: STRING AS token_address,
    event_data :amt :: INT AS amount_unadj,
     {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash']
    ) }} AS bridge_celer_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    a._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    LEFT JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
WHERE
    event_name = 'MintEvent'
