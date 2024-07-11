{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash,payload_function,sender);",
    tags = ['core','full_test']
) }}

SELECT
    b.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    A.data :success :: BOOLEAN AS success,
    A.tx_type,
    A.data :sender :: STRING AS sender,
    A.data :signature :: STRING AS signature,
    A.data :payload AS payload,
    A.data :payload :function :: STRING AS payload_function,
    A.data :changes AS changes,
    A.data :events AS events,
    A.data: gas_unit_price :: bigint AS gas_unit_price,
    A.data :gas_used :: INT AS gas_used,
    A.data :max_gas_amount :: bigint AS max_gas_amount,
    A.data :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
    A.data :vm_status :: STRING AS vm_status,
    A.data :state_change_hash :: STRING AS state_change_hash,
    A.data :accumulator_root_hash :: STRING AS accumulator_root_hash,
    A.data :event_root_hash :: STRING AS event_root_hash,
    A.data :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }} A
    JOIN {{ ref(
        'silver__blocks'
    ) }}
    b
    ON A.version BETWEEN b.first_version
    AND b.last_version

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        b.modified_timestamp
    ) >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
