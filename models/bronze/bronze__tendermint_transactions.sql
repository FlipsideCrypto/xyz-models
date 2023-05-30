{{ config(
    materialized = 'incremental',
    unique_key = 'block_number',
    cluster_by = ['_inserted_timestamp::date'],
    merge_update_columns = ["data", "_inserted_timestamp"],
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
                table_name => '{{ source( "bronze_streamline", "tendermint_transactions") }}'
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
    VALUE,
    _partition_by_block_id,
    block_number AS block_id,
    metadata,
    DATA,
    TO_TIMESTAMP(
        m._inserted_timestamp
    ) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'tendermint_transactions'
    ) }}
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    DATA: error IS NULL 

