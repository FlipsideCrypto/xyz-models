{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    _id AS tx_version,
    A.block_number
FROM
    {{ ref(
        'silver__blocks'
    ) }} A
    JOIN {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
    b
    ON b._id BETWEEN A.first_version
    AND A.last_version
WHERE
    tx_count_from_transactions_array <> tx_count_from_versions
