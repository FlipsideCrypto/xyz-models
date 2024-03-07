{{ config(
    materialized = 'incremental',
    unique_key = "nft_mints_combined_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_mints_v1') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    *
FROM
    {{ ref('silver__nft_mints_v2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
xfers AS (
    SELECT
        tx_hash,
        event_index,
        token_address
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    mints.block_timestamp,
    mints.block_number,
    mints.version,
    mints.tx_hash,
    mints.event_index,
    mints.event_type,
    mints.nft_address,
    mints.project_name,
    mints.nft_from_address,
    mints.nft_to_address,
    mints.tokenid,
    mints.token_version,
    mints.nft_count,
    mints.price_raw AS total_price_raw,
    transfers.token_address AS currency_address,
    {{ dbt_utils.generate_surrogate_key(
        ['mints.tx_hash','mints.event_index']
    ) }} AS nft_mints_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base mints
    JOIN xfers transfers
    ON mints.tx_hash = transfers.tx_hash qualify ROW_NUMBER() over (
        PARTITION BY mints.tx_hash,
        mints.event_index
        ORDER BY
            transfers.event_index DESC
    ) = 1
