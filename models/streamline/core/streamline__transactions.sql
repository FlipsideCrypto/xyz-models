{{ config (
    materialized = "incremental",
    unique_key = "tx_version",
    tags = ['streamline_view']
) }}

WITH all_versions AS (

    SELECT
        block_number,
        first_version,
        last_version,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__blocks'
        ) }}
    WHERE
        COALESCE(
            tx_count_from_transactions_array,
            0
        ) <> tx_count_from_versions

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    _id AS tx_version,
    _inserted_timestamp
FROM
    all_versions A
    JOIN {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
    b
    ON b._id BETWEEN A.first_version + 100
    AND A.last_version
