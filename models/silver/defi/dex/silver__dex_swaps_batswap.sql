{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_batswap_id",
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
        event_address IN (
            '0xe52923154e25c258d9befb0237a30b4001c63dc3bb73011c29cb3739befffcef',
            '0x6ee5ff12d9af89de4cb9f127bc4c484d26acda56c03536b5e3792eac94da0a36',
            '0x2ad8f7e64c7bffcfe94d7dea84c79380942c30e13f1b12c7a89e98df91d0599b'
        )
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
    A.event_data :user :: STRING AS swapper,
    CASE
        WHEN A.event_data :amount_x_in :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [1], '>', 1) :: STRING, ' ')
        WHEN A.event_data :amount_x_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_in,
    CASE
        WHEN A.event_data :amount_y_in :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [1], '>', 1) :: STRING, ' ')
        WHEN A.event_data :amount_y_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_out,
    CASE
        WHEN A.event_data: amount_x_in :: INT = 0 THEN A.event_data: amount_y_in :: INT
        ELSE A.event_data: amount_x_in :: INT
    END AS amount_in_unadj,
    CASE
        WHEN A.event_data: amount_y_out :: INT = 0 THEN A.event_data: amount_x_out :: INT
        ELSE A.event_data: amount_y_out :: INT
    END AS amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_batswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
