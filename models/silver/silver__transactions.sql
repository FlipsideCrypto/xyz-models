{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH base_table AS (

    SELECT
    tx.value :height :: INTEGER AS block_id, 
    tx_id, 
    tx.value :tx_result :codespace :: STRING as codespace,
    tx.value :tx_result :gas_used :: NUMBER as gas_used, 
    tx.value :tx_result :gas_wanted :: NUMBER as gas_wanted, 
    CASE
        WHEN tx.value :tx_result :code :: NUMBER = 0 THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    tx.value :tx_result :code :: NUMBER AS tx_code,
    tx.value :tx_result :events AS msgs,
    tx.value :tx_result :log AS tx_log,
    _inserted_timestamp,
    tx.value as val
FROM {{ ref('bronze__transactions') }} t, 
LATERAL FLATTEN(input => data :data :result :txs) tx

{% if is_incremental() %}
WHERE _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_id,
    block_timestamp,
    tx_id,
    codespace,
    gas_used,
    gas_wanted,
    tx_succeeded,
    tx_code,
    msgs,
    tx_log :: STRING AS tx_log,
    b._inserted_timestamp
FROM
    base_table b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id

{% if is_incremental() %}
WHERE
    bb._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
