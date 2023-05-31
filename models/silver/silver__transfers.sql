{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, block_id, msg_index, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__msg_attributes') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
tx AS (
    SELECT
        DISTINCT block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        _inserted_timestamp
    FROM
        base

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
evmos_txs AS (
    SELECT
        DISTINCT tx_id
    FROM
        base
    WHERE
        attribute_value IN (
            '/cosmos.bank.v1beta1.MsgSend',
            '/cosmos.bank.v1beta1.MsgMultiSend',
            '/ibc.applications.transfer.v1.MsgTransfer'
        )
),
sender AS (
    SELECT
        m.block_id,
        m.tx_id,
        m.msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        base m
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
msg_index AS (
    SELECT
        m.block_id,
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        evmos_txs v
        LEFT OUTER JOIN base m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index
),
receiver AS (
    SELECT
        m.block_id,
        v.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        evmos_txs v
        LEFT OUTER JOIN base m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'ibc_transfer'
        OR msg_type = 'transfer'
        AND attribute_key = 'recipient'
        AND m.msg_index > s.msg_index
),
amount AS (
    SELECT
        m.block_id,
        v.tx_id,
        m.msg_index,
        COALESCE(
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        attribute_value,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            TRY_PARSE_JSON(attribute_value) :amount
        ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))),
            TRY_PARSE_JSON(attribute_value) [1] :denom
        ) AS currency
    FROM
        evmos_txs v
        LEFT OUTER JOIN base m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index
),
evmos_txs_final AS (
    SELECT
        r.block_id,
        block_timestamp,
        r.tx_id,
        tx_succeeded,
        'EVMOS' AS transfer_type,
        r.msg_index,
        sender,
        amount,
        currency,
        receiver,
        _inserted_timestamp
    FROM
        receiver r
        LEFT OUTER JOIN amount C
        ON r.tx_id = C.tx_id
        AND r.block_id = C.block_id
        AND r.msg_index = C.msg_index
        LEFT OUTER JOIN sender s
        ON r.tx_id = s.tx_id
        AND r.block_id = s.block_id
        LEFT OUTER JOIN tx t
        ON r.tx_id = t.tx_id
        AND r.block_id = t.block_id
    WHERE
        amount IS NOT NULL
        AND sender IS NOT NULL
),
ibc_in_tx AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        'IBC_TRANSFER_IN' AS transfer_type,
        TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
        TRY_PARSE_JSON(attribute_value) :amount :: INT AS amount,
        CASE
            WHEN TRY_PARSE_JSON(attribute_value) :denom :: STRING LIKE '%/%' THEN SPLIT(TRY_PARSE_JSON(attribute_value) :denom :: STRING, '/') [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
            ELSE TRY_PARSE_JSON(attribute_value) :denom :: STRING
        END AS currency,
        TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
        _inserted_timestamp
    FROM
        base
    WHERE
        msg_type = 'write_acknowledgement'
        AND attribute_key = 'packet_data'
        AND TRY_PARSE_JSON(attribute_value): amount IS NOT NULL
),
ibc_out_txid AS (
    SELECT
        tx_id
    FROM
        base
    WHERE
        msg_type = 'ibc_transfer'
),
ibc_out_tx AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        'IBC_TRANSFER_OUT' AS transfer_type,
        TRY_PARSE_JSON(attribute_value) :sender :: STRING AS sender,
        TRY_PARSE_JSON(attribute_value) :amount :: INT AS amount,
        CASE
            WHEN TRY_PARSE_JSON(attribute_value) :denom :: STRING LIKE '%/%' THEN SPLIT(TRY_PARSE_JSON(attribute_value) :denom :: STRING, '/') [array_size(split(try_parse_json(attribute_value):denom::string, '/')) - 1]
            ELSE TRY_PARSE_JSON(attribute_value) :denom :: STRING
        END AS currency,
        TRY_PARSE_JSON(attribute_value) :receiver :: STRING AS receiver,
        _inserted_timestamp
    FROM
        base
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                ibc_out_txid
        )
        AND msg_type = 'send_packet'
        AND attribute_key = 'packet_data'
),
ibc_transfers_agg AS (
    SELECT
        *
    FROM
        ibc_out_tx
    UNION ALL
    SELECT
        *
    FROM
        ibc_in_tx
),
ibc_tx_final AS (
    SELECT
        i.block_id,
        i.block_timestamp,
        i.tx_id,
        i.tx_succeeded,
        i.transfer_type,
        i.sender,
        i.amount,
        i.currency,
        i.receiver,
        msg_index,
        _inserted_timestamp
    FROM
        ibc_transfers_agg i
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    receiver,
    msg_index,
    _inserted_timestamp
FROM
    ibc_tx_final
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    receiver,
    msg_index,
    _inserted_timestamp
FROM
    evmos_txs_final
