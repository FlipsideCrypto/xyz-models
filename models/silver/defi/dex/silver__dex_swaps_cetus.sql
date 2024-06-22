{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_cetus_id",
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
        event_type,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        event_address = '0xec42a352cc65eca17a9fa85d0fc602295897ed6b8b8af6a6c79ef490eb8f9eba'
        AND event_resource ILIKE 'SwapEvent%'
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    A.event_data :account AS swapper,
    CASE
        WHEN A.event_data :a_in :: INT = 0 THEN A.event_data :coin_b_info :account_address || ':' || A.event_data :coin_b_info :module_name || ':' || A.event_data :coin_b_info :struct_name
        WHEN A.event_data :a_in :: INT != 0 THEN A.event_data :coin_a_info :account_address || ':' || A.event_data :coin_a_info :module_name || ':' || A.event_data :coin_a_info :struct_name
    END AS token_in,
    CASE
        WHEN A.event_data :a_out :: INT = 0 THEN A.event_data :coin_b_info :account_address || ':' || A.event_data :coin_b_info :module_name || ':' || A.event_data :coin_b_info :struct_name
        WHEN A.event_data :a_out :: INT != 0 THEN A.event_data :coin_a_info :account_address || ':' || A.event_data :coin_a_info :module_name || ':' || A.event_data :coin_a_info :struct_name
    END AS token_out,
    CASE
        WHEN A.event_data: a_in :: INT = 0 THEN A.event_data: b_in :: INT
        ELSE A.event_data: a_in :: INT
    END AS amount_in_raw,
    CASE
        WHEN A.event_data: a_out :: INT = 0 THEN A.event_data: b_out :: INT
        ELSE A.event_data: a_out :: INT
    END AS amount_out_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_cetus_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
