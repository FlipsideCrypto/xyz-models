{{ config(
    materialized = 'incremental',
    unique_key = ['block_number','tx_hash','event_index'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}

SELECT
    A.block_number,
    -- A.block_height,
    A.block_timestamp,
    A.tx_hash,
    A.type AS tx_type,
    b.index AS event_index,
    b.value :type :: STRING AS event_type,
    b.value :data AS event_data,
    -- b.value :guid :: STRING AS event_guid, -- extract into account_address + creation_number
    b.value :guid :account_address :: STRING AS account_address,
    b.value :guid :creation_number :: STRING AS creation_number,
    b.value :sequence_number :: bigint AS sequence_number,
    -- b.value AS event,
    _inserted_timestamp
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
