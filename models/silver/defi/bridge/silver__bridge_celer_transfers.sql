{{ config(
    materialized = 'incremental',
    unique_key = "bridge_celer_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = [block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        *,
        SPLIT_PART(
            payload_function,
            '::',
            1
        ) AS bridge_address
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        bridge_address = '0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d'
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
        payload_function,
        '::',
        2
    ) AS FUNCTION,
    SPLIT_PART(
        payload_function,
        '::',
        3
    ) AS event_name,
    'celer' AS platform,
    sender,
    payload :arguments [2]::string AS destination_chain_receiver,
    payload :arguments [1] :: INT AS destination_chain_id,
    CASE
        WHEN payload :arguments [1] = '1' THEN 'Ethereum'
        WHEN payload :arguments [1] = '56' THEN 'BSC'
        WHEN left(payload :arguments [1],2) = '56' THEN 'BSC'
        ELSE 'Others'
    END AS destination_chain,
    payload :type_arguments [0] :: STRING AS token_address,
    payload :arguments [0] :: INT AS amount_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS bridge_celer_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base tr
WHERE
    FUNCTION = 'peg_bridge' -- bridge from aptos function in celer
    AND event_name = 'burn' -- bridge from aptos event_name in celer
