{{ config(
    materialized = 'ephemeral'
) }}

WITH pending_blocks AS (

    SELECT
        block_number,
        block_hash
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        _inserted_timestamp >= DATEADD(
            'day',
            -3,
            CURRENT_DATE
        )
        AND is_pending
)
SELECT
    *
FROM
    pending_blocks
