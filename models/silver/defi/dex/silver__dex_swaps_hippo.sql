{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_hippo_id",
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
            '0x890812a6bbe27dd59188ade3bbdbe40a544e6e104319b7ebc6617d3eb947ac07',
            '0x89576037b3cc0b89645ea393a47787bb348272c76d6941c574b053672b848039'
        )
        AND event_resource ILIKE 'SwapStepEvent%'
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
    COALESCE(
        A.event_data :user,
        b.sender
    ) AS swapper,
    REPLACE(
        REPLACE(
            utils.udf_hex_to_string(
                SUBSTRING(
                    A.event_data :x_type_info :struct_name,
                    3
                )
            ),
            'Coin<'
        ),
        '>'
    ) AS token_in,
    REPLACE(
        REPLACE(
            utils.udf_hex_to_string(
                SUBSTRING(
                    A.event_data :y_type_info :struct_name,
                    3
                )
            ),
            'Coin<'
        ),
        '>'
    ) AS token_out,
    A.event_data :input_amount :: INT AS amount_in_raw,
    A.event_data :output_amount :: INT AS amount_out_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_hippo_id,
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
