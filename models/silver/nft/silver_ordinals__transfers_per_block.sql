{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'block_hash',
    cluster_by = ["block_number"],
    tags = ["hiro_api"]
) }}

WITH blocks AS (

    SELECT
        block_number,
        block_hash,
        _inserted_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_number >= 767430
        AND NOT is_pending

{% if is_incremental() %}
AND block_number NOT IN (
    SELECT
        DISTINCT block_number
    FROM
        {{ this }}
    WHERE
        status_code = 200
)
{% endif %}
ORDER BY
    block_number ASC
LIMIT
    100
), 
get_inscription_count AS (
    SELECT
        block_number,
        block_hash,
        _inserted_timestamp,
        SYSDATE() AS _request_timestamp,
        {{ target.database }}.live.udf_api(
            'GET',
            'https://api.hiro.so/ordinals/v1/inscriptions/transfers?block=' || block_hash || '&limit=1',
            {},
            {}
        ) AS response
    FROM
        blocks
)
SELECT
    block_number,
    block_hash,
    response :data :total :: NUMBER AS transfer_count,
    response :status_code :: NUMBER AS status_code,
    _request_timestamp,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    get_inscription_count
