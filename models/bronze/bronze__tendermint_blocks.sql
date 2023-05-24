{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["block_id"],
) }}
SELECT
    value, 
    _partition_by_block_id,
    block_number AS block_id,
    DATA,
    TO_TIMESTAMP(
        _inserted_timestamp
    ) AS _inserted_timestamp
FROM
    {{ ref('bronze__streamline_FR_tendermint_blocks') }}

WHERE
    DATA: error IS NULL 

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
