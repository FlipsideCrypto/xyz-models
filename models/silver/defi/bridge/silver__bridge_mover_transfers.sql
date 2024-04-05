{{ config(
    materialized = 'incremental',
    unique_key = "bridge_mover_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        payload_function IN (
            '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_in',
            '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_out',
            '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_in',
            '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_out'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-03-12'
{% endif %}
),
events AS (
    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        payload_function IN (
            '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_in',
            '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_out',
            '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_in',
            '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_out'
        )
        AND event_module = 'bridge'
        AND event_resource IN (
            'SwapInEvent',
            'SwapOutEvent'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-03-12'
{% endif %}
) --bridge in
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'mover' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    'inbound' AS direction,
    b.sender AS tx_sender,
    event_data :from :: STRING AS sender,
    event_data :to :: STRING AS receiver,
    event_data :src_chain :: INT AS source_chain_id,
    SPLIT_PART(
        payload :type_arguments [1],
        '::',
        3
    ) :: STRING AS source_chain_name,
    137 AS destination_chain_id,
    'aptos' AS destination_chain_name,
    payload :type_arguments [0] :: STRING AS token_address,
    event_data :amount :: INT AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash']
    ) }} AS bridge_mover_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    events A
    LEFT JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
WHERE
    A.event_resource = 'SwapInEvent'
    AND b.payload_function IN (
        '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_in',
        '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_in'
    )
UNION ALL
    --bridge out
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'mover' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    'outbound' AS direction,
    b.sender AS tx_sender,
    b.sender AS sender,
    A.event_data :to :: STRING AS receiver,
    137 AS source_chain_id,
    'aptos' AS source_chain_name,
    A.event_data :dest_chain :: INT AS destination_chain_id,
    SPLIT_PART(
        payload :type_arguments [1],
        '::',
        3
    ) :: STRING AS destination_chain_name,
    payload :type_arguments [0] :: STRING AS token_address,
    COALESCE(TRY_CAST(payload :arguments [0] :: STRING AS INT), event_data :amount_before :: INT) AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash']
    ) }} AS bridge_mover_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    events A
    LEFT JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
WHERE
    A.event_resource = 'SwapOutEvent'
    AND b.payload_function IN (
        '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1::bridge::swap_out',
        '0xbb49903ab2f89554575df14adea91790b7dce260bdc8f6dab7edeee08b01fca5::bridge::swap_out'
    )
