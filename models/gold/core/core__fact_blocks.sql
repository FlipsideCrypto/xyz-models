{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    first_version,
    last_version,
    tx_count_from_versions AS tx_count,
    blocks_id AS fact_blocks_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__blocks'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
