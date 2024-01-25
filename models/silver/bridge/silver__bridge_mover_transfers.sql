{{ config(
    materialized = 'incremental',
    unique_key = "bridge_mover_transfers_id",
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
        bridge_address = '0xb3db6a8618db6e0eb9f2a3c98f693a7e622f986377c3153e6dd602ca74984ef1'
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-08-24'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    payload,
    SPLIT_PART(
        payload :function,
        '::',
        1
    ) AS bridge_address,
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
    'mover' AS platform,
    sender,
    payload :arguments [1] :: STRING AS destination_chain_receiver,
    SPLIT_PART(
        payload :type_arguments [1],
        '::',
        3
    ) :: STRING AS destination_chain,
    payload :arguments [2] :: INT AS destination_chain_id,
    payload :type_arguments [0] :: STRING AS token_address,
    payload :arguments [0] :: INT AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_mover_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base tr
WHERE
    FUNCTION = 'bridge' --bridge from aptos function in mover
    AND event_name = 'swap_out' --bridge from aptos event name in mover
