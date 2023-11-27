{{ config(
    materialized = 'incremental',
    incremental_stratege = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'day',
    tags = ['atlas']
) }}

WITH dates AS (

    SELECT
        date_day AS DAY
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}

{% if is_incremental() %}
WHERE
    date_day > (
        SELECT
            MAX(DAY)
        FROM
            {{ this }}
    )
    AND date_day < SYSDATE() :: DATE
{% else %}
WHERE
    date_day BETWEEN '2020-07-22'
    AND SYSDATE() :: DATE
{% endif %}
),
txns AS (
    SELECT
        block_timestamp :: DATE AS active_day,
        from_address
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            MAX(DAY)
        FROM
            {{ this }}
    ) - INTERVAL '30 days'
{% endif %}
),
FINAL AS (
    SELECT
        DAY,
        COUNT(
            DISTINCT from_address
        ) AS maa
    FROM
        dates d
        LEFT JOIN txns t
        ON t.active_day < d.day
        AND t.active_day >= d.day - INTERVAL '30 day'
    WHERE
        DAY != CURRENT_DATE()
    GROUP BY
        1
    ORDER BY
        1 DESC
)
SELECT
        {{ dbt_utils.generate_surrogate_key(
        ['day']
    ) }} AS atlas_maa_id,
    DAY,
    maa,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    FINAL
