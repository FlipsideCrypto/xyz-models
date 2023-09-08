{{ config(
    materialized = 'ephemeral'
) }}

WITH blocks AS (

    SELECT
        block_number,
        block_hash,
        tx_count
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        _inserted_timestamp >= DATEADD(
            'day',
            -3,
            CURRENT_DATE
        )
),
transactions AS (
    SELECT
        block_number,
        COUNT(
            DISTINCT tx_id
        ) AS tx_count
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        _inserted_timestamp >= DATEADD(
            'day',
            -3,
            CURRENT_DATE
        )
    GROUP BY
        1
)
SELECT
    b.block_number,
    b.block_hash
FROM
    blocks b
    LEFT JOIN transactions t USING (block_number)
WHERE
    b.tx_count != t.tx_count
    OR t.tx_count IS NULL
