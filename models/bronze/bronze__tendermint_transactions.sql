{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["data", "_inserted_timestamp"],
) }}

SELECT
    value, 
    _partition_by_block_id,
    block_number as block_id,
    value :data :hash :: STRING AS tx_id,
    DATA,
    TO_TIMESTAMP(
        _inserted_timestamp
    ) AS _inserted_timestamp
FROM
    {{ ref('bronze__streamline_FR_tendermint_transactions') }}
WHERE
    DATA: error IS NULL qualify(ROW_NUMBER() over (PARTITION BY value :data :hash :: STRING
ORDER BY
    _inserted_timestamp DESC)) = 1
