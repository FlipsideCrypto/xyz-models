{{ config(
    materialized = 'incremental',
    unique_key = "bridge_wormhole_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        *,
        SPLIT_PART(
            payload :function,
            '::',
            1
        ) AS bridge_address
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        bridge_address = '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f'
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
),
wormhole_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        -- payload:function data structure is as follows ==> bridge_address::function::event_name
        payload,
        bridge_address,
        SPLIT_PART(
            payload :function,
            '::',
            2
        ) AS function,
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
                ) = '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f' THEN 'Wormhole' -- wormhole bridge address
                ELSE 'Others'
            END
        ) AS platform,
        sender,
        (
            CASE
                WHEN LEFT(
                    payload :arguments [2],
                    26
                ) = '0x000000000000000000000000' THEN CONCAT('0x', RIGHT(payload :arguments [2], 40))
                ELSE payload :arguments [2]
            END
        ) AS destination_recipient_address,
        chain_name AS destination_chain,
        payload :arguments [1] :: INT AS destination_chain_id,
        payload :type_arguments [0] :: STRING AS token_address,
        payload :arguments [0] :: INT AS amount_unadj,
        _inserted_timestamp
    FROM
        base
        LEFT JOIN {{ ref('silver__bridge_wormhole_chain_id_seed') }}
        ON chain_id = destination_chain_id
    WHERE
        function = 'transfer_tokens' --bridge from aptos function in wormhole
        AND event_name = 'transfer_tokens_entry' --bridge from aptos event name in wormhole
),
near_addresses AS (
    SELECT
        near_address,
        addr_encoded
    FROM
        crosschain.silver.near_address_encoded
)
SELECT
    t.block_number,
    t.block_timestamp,
    t.version,
    t.tx_hash,
    t.payload,
    t.bridge_address,
    t.function,
    t.event_name,
    t.platform,
    t.sender,
    t.destination_recipient_address,
    t.destination_chain,
    t.destination_chain_id,
    CASE
        WHEN destination_chain = 'solana' THEN ethereum.utils.udf_hex_to_base58(destination_recipient_address)
        WHEN destination_chain IN (
            'injective',
            'sei'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            destination_recipient_address,
            SUBSTR(
                destination_chain,
                1,
                3
            )
        )
        WHEN destination_chain IN (
            'osmosis',
            'xpla'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            destination_recipient_address,
            SUBSTR(
                destination_chain,
                1,
                4
            )
        )
        WHEN destination_chain IN (
            'terra',
            'terra2',
            'evmos'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            destination_recipient_address,
            SUBSTR(
                destination_chain,
                1,
                5
            )
        )
        WHEN destination_chain IN (
            'cosmoshub',
            'kujira'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            destination_recipient_address,
            SUBSTR(
                destination_chain,
                1,
                6
            )
        )
        WHEN destination_chain IN ('near') THEN near_address
        WHEN destination_chain IN ('algorand') THEN ethereum.utils.udf_hex_to_algorand(destination_recipient_address)
        WHEN destination_chain IN ('polygon') THEN SUBSTR(
            destination_recipient_address,
            1,
            42
        )
        ELSE destination_recipient_address
    END AS destination_chain_receiver,
    t.token_address,
    t.amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_wormhole_transfers_id, -- tx_id is unique but is it enough?
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    t._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wormhole_transfers t
    LEFT JOIN near_addresses n
    ON t.destination_recipient_address = n.addr_encoded
