{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, block_id, msg_index, currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH evmos_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_value IN (
            '/cosmos.bank.v1beta1.MsgSend',
            '/cosmos.bank.v1beta1.MsgMultiSend', 
            '/ibc.applications.transfer.v1.MsgTransfer'
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
sender_index AS (
    SELECT
        tx_id,
        MIN(msg_index) AS msg_index
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    tx_id
),
sender AS (
    SELECT
        m.block_id,
        m.tx_id,
        s.msg_index,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        {{ ref('silver__msg_attributes') }}
        m
        INNER JOIN sender_index s
        ON m.tx_id = s.tx_id
        AND m.msg_index = s.msg_index
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
msg_index AS (
    SELECT
        m.block_id,
        v.tx_id,
        attribute_key,
        m.msg_index
    FROM
        evmos_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
receiver AS (
    SELECT
        m.block_id,
        v.tx_id,
        m.msg_index,
        attribute_value AS receiver
    FROM
        evmos_txs v
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'ibc_transfer'
        OR msg_type = 'transfer'
        AND attribute_key = 'recipient'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
        LEFT OUTER JOIN {{ ref('silver__msg_attributes') }}
        m
        ON v.tx_id = m.tx_id
        INNER JOIN sender s
        ON v.tx_id = s.tx_id
        AND m.block_id = s.block_id
    WHERE
        msg_type = 'transfer'
        AND attribute_key = 'amount'
        AND m.msg_index > s.msg_index

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
        LEFT OUTER JOIN {{ ref('silver__transactions') }}
        t
        ON r.tx_id = t.tx_id
        AND r.block_id = t.block_id
    WHERE 
        amount IS NOT NULL
        AND sender IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'write_acknowledgement'
        AND attribute_key = 'packet_data'
        AND TRY_PARSE_JSON(attribute_value): amount IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
ibc_out_txid AS (
    SELECT
        tx_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'ibc_transfer'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
        {{ ref('silver__msg_attributes') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                ibc_out_txid
        )
        AND msg_type = 'send_packet'
        AND attribute_key = 'packet_data'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
