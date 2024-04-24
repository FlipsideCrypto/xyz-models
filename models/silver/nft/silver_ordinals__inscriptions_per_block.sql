{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'block_hash',
    cluster_by = ["block_number"],
    tags = ["hiro_api"]
) }}

WITH starting_block AS (

    {% if is_incremental() %}

    SELECT
        MAX(block_number) AS from_height, 
        LEAST(bitcoin.streamline.udf_get_chainhead(), from_height + 5000) AS to_height
    FROM
        {{ this }}
    {% else %}
    SELECT
        767430 AS from_height, 
        772430 AS to_height
    {% endif %}

),
get_inscription_count AS (
    SELECT
        SYSDATE() AS _request_timestamp,
        {{ target.database }}.live.udf_api(
            'GET',
            'https://api.hiro.so/ordinals/v1/stats/inscriptions?from_block_height=' || from_height || '&to_block_height=' || to_height,
            {},
            {}
        ) AS response
    FROM
        starting_block
)
SELECT
    r.value :block_height :: INTEGER AS block_number,
    r.value :block_hash :: STRING AS block_hash,
    r.value :inscription_count :: INTEGER AS inscription_count,
    r.value :inscription_count_accum :: INTEGER AS inscription_count_accum,
    (
        r.value :timestamp :: bigint / 1000
    ) :: timestamp_ntz AS block_timestamp,
    _request_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    get_inscription_count,
    LATERAL FLATTEN(
        input => response :data :results :: variant
    ) r
