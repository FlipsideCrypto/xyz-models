{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.bulk_get_tendermint_validators(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_number
    {% else %}
    SELECT
        MAX(block_number) - 100000 AS block_number --aprox 3 days
    FROM
        {{ ref("streamline__blocks") }}
    {% endif %}),
    tbl AS (
        SELECT
            block_number
        FROM
            {{ ref("streamline__blocks") }}
        WHERE
            (
                block_number >= (
                    SELECT
                        block_number
                    FROM
                        last_3_days
                )
            )
            AND block_number IS NOT NULL
        EXCEPT
        SELECT
            block_number
        FROM
            {{ ref("streamline__complete_tendermint_validators") }}
        WHERE
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
    )
SELECT
    block_number
FROM
    tbl
