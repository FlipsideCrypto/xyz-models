{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    version,
    success,
    A.tx_type,
    A.payload_function,
    b.index AS event_index,
    b.value :type :: STRING AS event_type,
    SPLIT_PART(
        event_type,
        '::',
        1
    ) :: STRING AS event_address,
    SPLIT_PART(
        event_type,
        '::',
        2
    ) :: STRING AS event_module,
    SPLIT_PART(
        event_type,
        '::',
        3
    ) :: STRING AS event_resource,
    b.value :data AS event_data,
    -- b.value :guid :: STRING AS event_guid, -- extract into account_address + creation_number
    b.value :guid :account_address :: STRING AS account_address,
    b.value :guid :creation_number :: bigint AS creation_number,
    b.value :sequence_number :: bigint AS sequence_number,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS events_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'core__fact_transactions'
    ) }} A,
    LATERAL FLATTEN (events) b

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
