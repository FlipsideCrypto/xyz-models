{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
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
    A.data :failed_proposer_indices :: STRING AS failed_proposer_indices,
    A.data :id :: STRING AS id,
    A.data :previous_block_votes_bitvec :: STRING AS previous_block_votes_bitvec,
    A.data :proposer :: STRING AS proposer,
    A.data :ROUND :: INT AS ROUND,
    A.data :vm_status :: STRING AS vm_status,
    A.data :state_change_hash :: STRING AS state_change_hash,
    A.data :accumulator_root_hash :: STRING AS accumulator_root_hash,
    A.data :event_root_hash :: STRING AS event_root_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_block_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }} A
    JOIN {{ ref('silver__blocks') }}
    b
    ON A.version BETWEEN b.first_version
    AND b.last_version
WHERE
    LEFT(
        tx_type,
        5
    ) = 'block'

{% if is_incremental() %}
AND GREATEST(
    A.modified_timestamp,
    b.modified_timestamp
) >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
