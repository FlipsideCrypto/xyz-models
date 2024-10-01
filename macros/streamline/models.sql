{% macro streamline_external_table_query_v2(
        model,
        partition_function
    ) %}
    
    {% set days = var("BRONZE_LOOKBACK_DAYS")%}
  
    WITH meta AS (
        SELECT
            last_modified AS inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -ABS({{days}}), CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            inserted_timestamp
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key
        WHERE
            b.partition_key = s.partition_key
            AND DATA :error IS NULL
{% endmacro %}

{% macro streamline_external_table_FR_query_v2(
        model,
        partition_function
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS inserted_timestamp,
            file_name,
            {{ partition_function }} AS partition_key
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    s.*,
    b.file_name,
    inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.partition_key = s.partition_key
WHERE
    b.partition_key = s.partition_key
    AND DATA :error IS NULL
{% endmacro %}
