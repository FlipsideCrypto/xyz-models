{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    first_version,
    last_version,
    tx_count_from_versions AS tx_count,
    _ID,
    _INSERTED_TIMESTAMP,
    _MD,
    invocation_id
FROM
    {{ ref(
        'silver__blocks'
    ) }}
