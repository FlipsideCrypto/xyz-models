{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index'],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, event_type,event_address,event_module,event_resource);"
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    version,
    success,
    A.tx_type,
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
    b.value :guid :creation_number :: STRING AS creation_number,
    b.value :sequence_number :: bigint AS sequence_number,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS _id,
    _inserted_timestamp,
    SYSDATE() AS _md,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref(
        'silver__transactions'
    ) }} A,
    LATERAL FLATTEN (events) b

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
