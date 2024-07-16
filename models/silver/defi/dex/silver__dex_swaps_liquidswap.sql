{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_liquidswap_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH tx AS (

    SELECT
        tx_hash,
        block_timestamp,
        sender
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
evnts AS (
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
            '0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12',
            '0x163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e'
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
    b.sender AS swapper,
    CASE
        WHEN A.event_data :x_in :: INT = 0 THEN TRIM(SPLIT(A.event_type, ',') [1] :: STRING, ' ')
        WHEN A.event_data :x_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_in,
    CASE
        WHEN A.event_data :y_in :: INT = 0 THEN TRIM(SPLIT(A.event_type, ',') [1] :: STRING, ' ')
        WHEN A.event_data :y_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_out,
    CASE
        WHEN A.event_data: x_in :: INT = 0 THEN A.event_data: y_in :: INT
        ELSE A.event_data: x_in :: INT
    END AS amount_in_unadj,
    CASE
        WHEN A.event_data: y_out :: INT = 0 THEN A.event_data: x_out :: INT
        ELSE A.event_data: y_out :: INT
    END AS amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_liquidswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    JOIN tx b USING(
        tx_hash,
        block_timestamp
    )
