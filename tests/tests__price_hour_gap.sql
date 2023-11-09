{{ config(
    severity = 'error'
) }}

WITH prices AS (

    SELECT
        HOUR,
        LAG(HOUR) over (
            ORDER BY
                HOUR
        ) AS prev_hour
    FROM
        {{ ref('price__fact_hourly_token_prices') }}
    {% if not var('FULL_TEST', False) %}
    WHERE hour >= SYSDATE() - interval '24 hours'
    {% endif %}
)
SELECT
    *
FROM
    prices
WHERE
    DATEDIFF(
        'hour',
        prev_hour,
        HOUR
    ) > 1
