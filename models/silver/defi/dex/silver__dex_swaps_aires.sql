{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_aires_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore'],
    enabled = false
) }}
-- pretty sure this is an aggregator (not included for now)
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
        event_module = 'controller'
        AND event_resource ILIKE 'SwapEvent%'
        AND event_address = '0x9770fa9c725cbd97eb50b2be5f7416efdfd1f1554beb0750d4dae4c64e860da3'
        AND success
        AND block_timestamp :: DATE >= '2024-06-01'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
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
    event_index,
    event_resource,
    event_data,
    event_data :sender :: STRING AS user_address,
    event_data :amount_in :: NUMBER AS amount,
    SPLIT(REGEXP_SUBSTR(event_type, '<([^>]*)>', 1, 1, 'e'), ', ') [0] :: STRING AS asset,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_aires_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts
