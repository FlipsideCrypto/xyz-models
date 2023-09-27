{{ config(
    materialized = 'view',
    tags = ['entity_cluster']
) }}

WITH base AS (

    SELECT
        DISTINCT tx_id,
        LOWER(pubkey_script_address) AS input_address,
        COUNT(
            DISTINCT pubkey_script_address
        ) over (
            PARTITION BY tx_id
        ) AS max_index
    FROM
        {{ ref('silver__inputs_final') }}
    WHERE
        tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                {{ ref('silver__inputs_final') }}
            WHERE
                LOWER(pubkey_script_address) IN (
                    SELECT
                        DISTINCT LOWER(address)
                    FROM
                        {{ ref("silver__labels") }}
                    WHERE
                        label_type != 'nft'
                )
        )
        AND DATE_TRUNC(
            'day',
            block_timestamp
        ) > CURRENT_DATE - interval '1460 days'
),
base2 AS (
    SELECT
        *
    FROM
        base
    WHERE
        max_index > 1
)
SELECT
    tx_id,
    ARRAY_AGG(input_address) within GROUP (
        ORDER BY
            tx_id DESC
    ) :: STRING AS address_array
FROM
    base2
GROUP BY
    tx_id
