{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_thala_v0_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE']
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
        AND block_timestamp :: DATE <= '2023-04-05'
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
        event_address IN (
            '0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af',
            '0x6970b4878c3aea96732be3f31c2dded12d94d9455ff0c76c67d84859dce35136'
        )
        AND event_resource LIKE 'SwapEvent%'
        AND success
        AND block_timestamp :: DATE <= '2023-04-05'
),
xfers AS (
    SELECT
        block_timestamp,
        tx_hash,
        amount,
        token_address,
        transfer_event
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        success
        AND block_timestamp :: DATE <= '2023-04-05'
),
fin AS (
    SELECT
        block_number,
        A.block_timestamp,
        version,
        A.tx_hash,
        event_index,
        event_address,
        b.sender AS swapper,
        event_data :amount_in :: INT AS amount_in_unadj,
        event_data :amount_out :: INT AS amount_out_unadj,
        c_in.token_address AS token_in,
        c_out.token_address AS token_out
    FROM
        evnts A
        JOIN tx b USING(
            tx_hash,
            block_timestamp
        )
        JOIN xfers c_in
        ON A.block_timestamp :: DATE = c_in.block_timestamp :: DATE
        AND A.tx_hash = c_in.tx_hash
        AND amount_in_unadj = c_in.amount
        AND c_in.transfer_event = 'WithdrawEvent'
        JOIN xfers c_out
        ON A.block_timestamp :: DATE = c_out.block_timestamp :: DATE
        AND A.tx_hash = c_out.tx_hash
        AND amount_out_unadj = c_out.amount
        AND c_out.transfer_event = 'DepositEvent'
    WHERE
        event_data :idx_in IS NULL
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
    ) }} AS dex_swaps_thala_v0_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
