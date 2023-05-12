{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["block_id"],
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "streamline", "tendermint_blocks") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    )
{% else %}
)
{% endif %}

SELECT
    value, 
    _partition_by_block_id,
    block_number AS block_id,
    DATA,
    TO_TIMESTAMP(
        m._inserted_timestamp
    ) AS _inserted_timestamp
FROM
    {{ source(
        'streamline',
        'tendermint_blocks'
    ) }}
JOIN meta m
ON m.file_name = metadata$filename
WHERE
    DATA: error IS NULL 

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
