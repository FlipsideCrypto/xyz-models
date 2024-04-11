-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_blocks']
) }}

WITH node_calls AS (
    -- generate a list of URLs for API calls and assign a batch number to each
    SELECT
        '{service}/{Authentication}/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
        block_height,
        CEIL(ROW_NUMBER() OVER(ORDER BY block_height DESC) / 10.0) AS batch_number
    FROM
        (
            SELECT
                block_number AS block_height
            FROM
                {{ ref('streamline__aptos_blocks') }}
            EXCEPT
            SELECT
                block_number
            FROM
                aptos.streamline.complete_blocks_tx
        )
    ORDER BY block_height DESC
),
batches AS (
    -- group URLs by batch number and calculate the partition key
    SELECT
        batch_number,
        ARRAY_AGG(calls) AS calls,
        ROUND(AVG(block_height),-3) AS partition_key
    FROM
        node_calls
    GROUP BY
        batch_number
),
lq_calls AS (
    SELECT
        CURRENT_TIMESTAMP(3) AS _inserted_timestamp,
        partition_key,
        {{ target.database }}.live.udf_api(
            'GET', -- request method
            t.VALUE, -- request url
            {}, -- request headers
            {}, -- request body
            'vault/dev/aptos/node/mainnet'
        ) AS request
    FROM
        batches,
        TABLE(FLATTEN(input => calls)) AS t(VALUE)
),
parsed_responses AS (
    SELECT
        partition_key,
        PARSE_JSON(request):"data":"block_height"::STRING AS block_number,
        PARSE_JSON(request):"data":"block_hash"::STRING AS block_hash,
        PARSE_JSON(request):"data":"block_timestamp"::STRING AS block_timestamp_num,
        TO_TIMESTAMP(
            block_timestamp_num :: STRING
        ) AS block_timestamp,
        PARSE_JSON(request):"data":"first_version"::STRING AS first_version,
        PARSE_JSON(request):"data":"last_version"::STRING AS last_version,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        'LQ' AS source
    FROM
        lq_calls
)
SELECT * FROM (
    SELECT
        partition_key,
        DATA :block_height :: STRING AS block_number,
        DATA :block_hash :: STRING AS block_hash,
        DATA :block_timestamp :: bigint AS block_timestamp_num,
        TO_TIMESTAMP(
            block_timestamp_num :: STRING
        ) AS block_timestamp,
        DATA :first_version :: bigint AS first_version,
        DATA :last_version :: bigint AS last_version,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        'APTOS' AS source
    FROM
    {% if is_incremental() %}
    {{ ref('bronze__streamline_blocks_tx') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__streamline_FR_blocks_tx') }}
    {% endif %}
    qualify(ROW_NUMBER() over(PARTITION BY block_number
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
UNION ALL
SELECT * FROM parsed_responses