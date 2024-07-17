{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_cetus_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH tx AS (

    SELECT
        tx_hash,
        block_timestamp,
        sender,
        modified_timestamp
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        success
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
        modified_timestamp
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        event_address = '0xec42a352cc65eca17a9fa85d0fc602295897ed6b8b8af6a6c79ef490eb8f9eba'
        AND event_resource ILIKE 'SwapEvent%'
        AND success
),
pre_final AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        event_address,
        COALESCE(NULLIF(A.event_data :account :: STRING, '0x0'), b.sender) AS swapper,
        {# event_data, #}
        A.event_data :coin_b_info :account_address :: STRING AS coin_b_info_account_address,
        utils.udf_hex_to_string(
            SUBSTRING(
                A.event_data :coin_b_info :module_name,
                3
            )
        ) AS coin_b_info_module_name,
        utils.udf_hex_to_string(
            SUBSTRING(
                A.event_data :coin_b_info :struct_name,
                3
            )
        ) AS coin_b_info_struct_name,
        A.event_data :coin_a_info :account_address :: STRING AS coin_a_info_account_address,
        utils.udf_hex_to_string(
            SUBSTRING(
                A.event_data :coin_a_info :module_name,
                3
            )
        ) AS coin_a_info_module_name,
        utils.udf_hex_to_string(
            SUBSTRING(
                A.event_data :coin_a_info :struct_name,
                3
            )
        ) AS coin_a_info_struct_name,
        A.event_data :a_in :: INT AS a_in,
        A.event_data :b_in :: INT AS b_in,
        A.event_data :a_out :: INT AS a_out,
        A.event_data :b_out :: INT AS b_out,
        coin_b_info_account_address || '::' || coin_b_info_module_name || '::' || coin_b_info_struct_name AS coin_b_token,
        coin_a_info_account_address || '::' || coin_a_info_module_name || '::' || coin_a_info_struct_name AS coin_a_token,
        CASE
            WHEN a_in = 0 THEN coin_b_token
            WHEN a_in != 0 THEN coin_a_token
        END AS token_in,
        CASE
            WHEN a_out = 0 THEN coin_b_token
            WHEN a_out != 0 THEN coin_a_token
        END AS token_out,
        CASE
            WHEN a_in = 0 THEN b_in
            ELSE a_in
        END AS amount_in_unadj,
        CASE
            WHEN a_out = 0 THEN b_out
            ELSE a_out
        END AS amount_out_unadj
    FROM
        evnts A
        JOIN tx b USING(
            tx_hash,
            block_timestamp
        )

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        b.modified_timestamp
    ) >= (
        SELECT
            MAX(modified_timestamp)
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
    swapper,
    token_in,
    token_out,
    amount_in_unadj,
    amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_cetus_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
