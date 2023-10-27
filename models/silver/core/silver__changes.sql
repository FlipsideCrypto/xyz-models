{{ config(
    materialized = 'incremental',
    unique_key = ['block_number','tx_hash','change_index'],
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.type AS tx_type,
    b.index AS change_index,
    b.value :data :data AS change_data,
    b.value :type :: STRING AS change_type,
    b.value :address :: STRING AS address,
    b.value :handle :: STRING AS handle,
    b.value :data: type :: STRING AS inner_change_type,
    b.value :key :: STRING AS key,
    b.value :value :: STRING AS VALUE,
    b.value :state_key_hash :: STRING AS state_key_hash,
    _inserted_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }} A,
    LATERAL FLATTEN (
        changes
    ) b
