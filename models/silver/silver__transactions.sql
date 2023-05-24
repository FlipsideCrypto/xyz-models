{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
) }}

WITH base_transactions AS (

    SELECT
        block_id,
        tx_id,
        data :tx_result :codespace AS codespace,
        data :tx_result :gas_used :: NUMBER AS gas_used,
        data :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN data :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        data :tx_result :code :: NUMBER AS tx_code,
        data :tx_result :events AS msgs,
        data :tx_result :log :: STRING AS tx_log,
        _inserted_timestamp
    FROM
        {{ ref('bronze__tendermint_transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
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
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_id
    ORDER BY
        t._inserted_timestamp DESC
) = 1
