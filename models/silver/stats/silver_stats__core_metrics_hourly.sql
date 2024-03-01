{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['scheduled_non_core']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('silver__transactions_final') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

WITH input_address AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        COUNT (
            DISTINCT pubkey_script_address
        ) AS input_address_count
    FROM
        {{ ref('silver__inputs_final') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC('hour', systimestamp())

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
),
output_address AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        COUNT (
            DISTINCT pubkey_script_address
        ) AS output_address_count
    FROM
        {{ ref('silver__outputs') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
),
transactions AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        MIN(block_number) AS block_number_min,
        MAX(block_number) AS block_number_max,
        COUNT(
            DISTINCT block_number
        ) AS block_count,
        COUNT(
            DISTINCT tx_hash
        ) AS transaction_count,
        SUM(fee) AS total_fees,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__transactions_final') }} AS txs
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
)
SELECT
    txs.*,
    input_address_count AS unique_from_count,
    output_address_count AS unique_to_count,
    {{ dbt_utils.generate_surrogate_key(
        ['txs.block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transactions AS txs
    LEFT JOIN input_address AS input
    ON txs.block_timestamp_hour = input.block_timestamp_hour
    LEFT JOIN output_address AS output
    ON txs.block_timestamp_hour = output.block_timestamp_hour
