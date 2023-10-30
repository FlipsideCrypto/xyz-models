{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','change_index'],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, change_type,inner_change_type,change_address,change_module,change_resource);"
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.tx_type,
    b.index AS change_index,
    b.value :data :data AS change_data,
    b.value :type :: STRING AS change_type,
    b.value :address :: STRING AS address,
    b.value :handle :: STRING AS handle,
    b.value :data: "type" :: STRING AS inner_change_type,
    SPLIT_PART(
        inner_change_type,
        '::',
        1
    ) :: STRING AS change_address,
    SPLIT_PART(
        inner_change_type,
        '::',
        2
    ) :: STRING AS change_module,
    SPLIT_PART(
        inner_change_type,
        '::',
        3
    ) :: STRING AS change_resource,
    b.value :key :: STRING AS key,
    b.value :value :: STRING AS VALUE,
    b.value :state_key_hash :: STRING AS state_key_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','change_index']
    ) }} AS _id,
    _inserted_timestamp,
    SYSDATE() AS _md,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref(
        'silver__transactions'
    ) }} A,
    LATERAL FLATTEN (changes) b

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
