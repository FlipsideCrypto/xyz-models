{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

SELECT
    b.block_id,
    block_timestamp,
    tx_id,
    b.data :tx_result :codespace :: STRING AS codespace,
    b.data :tx_result :gas_used :: NUMBER AS gas_used,
    b.data :tx_result :gas_wanted :: NUMBER AS gas_wanted,
    CASE
        WHEN b.data :tx_result :code :: NUMBER = 0 THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    b.data :tx_result :code :: NUMBER AS tx_code,
    b.data :tx_result :events AS msgs,
    TRY_BASE64_DECODE_STRING(
        DATA :tx_result :data
    ) AS tx_type,
    TRY_PARSE_JSON(
        b.data :tx_result :log
    ) AS tx_log,
    b._inserted_timestamp
FROM
    {{ ref('bronze__tendermint_transactions') }}
    b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id

WHERE tx_id is not null
{% if is_incremental() %}
AND
    bb._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
    AND b._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
