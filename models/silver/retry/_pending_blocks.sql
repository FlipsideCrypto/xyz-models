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
),
blocks_hash AS (
    SELECT
        block_number,
        block_hash
    FROM
        {{ ref('streamline__complete_blocks_hash') }}
)
SELECT
    block_number,
    COALESCE(
        p.block_hash,
        b.block_hash
    ) AS block_hash
FROM
    pending_blocks p
    LEFT JOIN blocks_hash b USING (block_number)
