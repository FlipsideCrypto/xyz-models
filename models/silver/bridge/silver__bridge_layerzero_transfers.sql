{{ config(
    materialized = 'incremental',
    unique_key = "bridge_layerzero_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        *,
         split_part(payload:function,'::',1) AS bridge_address
    FROM
        {{ ref(
        'silver__transactions'
    ) }}
    WHERE
        bridge_address = '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa'
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
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    payload,
    bridge_address,
    SPLIT_PART(
        payload :function,
        '::',
        2
    ) AS FUNCTION,
    SPLIT_PART(
        payload :function,
        '::',
        3
    ) AS event_name,
    (
        CASE
            WHEN SPLIT_PART(
                payload :function,
                '::',
                1
            ) = '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa' THEN 'layerzero' -- layerzero bridge address
            ELSE 'Others'
        END
    ) AS platform,
    sender,
    (
        CASE
            WHEN LEFT(
                payload :arguments [1],
                26
            ) = '0x000000000000000000000000' THEN CONCAT('0x', RIGHT(payload :arguments [1], 40))
            ELSE payload :arguments [1]
        END
    ) AS destination_chain_receiver,
    chain_name AS destination_chain,
    payload :arguments [0] AS destination_chain_id,
    payload :type_arguments [0] AS token_address,
    payload :arguments [2] AS amount_unadj,
        {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_layerzero_transfers_id, -- tx_id is unique but is it enough?
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base tr
    LEFT JOIN {{ ref('silver__bridge_layerzero_chain_id_seed') }}
    ON chain_id = destination_chain_id
WHERE
    success and FUNCTION = 'coin_bridge' -- bridge from aptos function in layerzero
    AND event_name = 'send_coin_from' -- bridge from aptos event_name in layer
