-- tests whether the number of transactions in the model matches the number of transactions in the block header
-- excludes last hour to avoid a false alert if the test runs while transactions is processing and not finalized

{% test tx_gaps(
    model,
    column_name,
    column_block,
    column_tx_count
) %}
WITH block_base AS (
    SELECT
        {{ column_block }},
        {{ column_tx_count }},
        _inserted_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        _inserted_timestamp <= SYSDATE() - INTERVAL '2 hours'
),
model_name AS (
    SELECT
        {{ column_block }},
        COUNT(
            DISTINCT {{ column_name }}
        ) AS model_tx_count
    FROM
        {{ model }}
    GROUP BY
        {{ column_block }}
)
SELECT
    block_base.{{ column_block }},
    {{ column_tx_count }},
    model_name.{{ column_block }} AS model_block_number,
    model_tx_count
FROM
    block_base
    LEFT JOIN model_name
    ON block_base.{{ column_block }} = model_name.{{ column_block }}
WHERE
    {{ column_tx_count }} <> model_tx_count
{% endtest %}
