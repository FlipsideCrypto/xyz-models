{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}
-- depends_on: {{ ref('bronze__streamline_tendermint_transactions') }}
WITH base_transactions AS (

    SELECT
        block_number AS block_id,
        COALESCE(
            t.value :tx_result :tx_id,
            t.value :hash
        ) :: STRING AS tx_id,
        t.value :tx_result :codespace :STRING AS codespace,
        t.value :tx_result :gas_used :: NUMBER AS gas_used,
        t.value :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN t.value :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        t.value :tx_result :code :: NUMBER AS tx_code,
        t.value :tx_result :events AS msgs,
        TRY_PARSE_JSON(
            t.value :tx_result :log
        ) AS tx_log,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tendermint_transactions') }}
tt
{% else %}
    {{ ref('bronze__streamline_FR_tendermint_transactions') }}
    tt
{% endif %},
TABLE(FLATTEN(DATA :result :txs)) t
WHERE
    tx_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    t.block_id,
    b.block_timestamp,
    tx_id,
    codespace,
    gas_used,
    gas_wanted,
    tx_succeeded,
    tx_code,
    msgs,
    tx_log,
    t._inserted_timestamp,
    concat_ws(
        '-',
        t.block_id,
        tx_id
    ) AS unique_key
FROM
    base_transactions t
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    b
    ON t.block_id = b.block_id

{% if is_incremental() %}
WHERE
    b._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 3
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_id
    ORDER BY
        t._inserted_timestamp DESC
) = 1
