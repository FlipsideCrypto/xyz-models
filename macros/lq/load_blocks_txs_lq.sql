{% macro load_blocks_txs_lq() %}
    {% set load_query %}
INSERT INTO
    bronze.lq_blocks_txs WITH gen AS (
        SELECT
            ROW_NUMBER() over (
                ORDER BY
                    SEQ4()
            ) AS block_height
        FROM
            TABLE(GENERATOR(rowcount => 106157220))
    ),
    blocks AS (
        SELECT
            block_height
        FROM
            gen
        ORDER BY
            1 DESC
    ),
    calls AS (
        SELECT
            'https://twilight-silent-gas.aptos-mainnet.quiknode.pro/f64d711fb5881ce64cf18a31f796885050178031/v1/blocks/by_height/' || block_height || '?with_transactions=true' calls,
            block_height
        FROM
            (
                SELECT
                    block_height
                FROM
                    blocks
                EXCEPT
                SELECT
                    block_height
                FROM
                    bronze.lq_blocks_txs A
                ORDER BY
                    1 DESC
                LIMIT
                    75
            )
    ), results AS (
        SELECT
            block_height,
            live.udf_api(
                'GET',
                calls,
                OBJECT_CONSTRUCT(
                    'Content-Type',
                    'application/json'
                ),{}
            ) DATA
        FROM
            calls
    )
SELECT
    *,
    SYSDATE() AS _inserted_timestamp
FROM
    results;
{% endset %}
    {% do run_query(load_query) %}
    {# {% set wait %}
    CALL system $ wait(10);
{% endset %}
    {% do run_query(wait) %}
    #}
{% endmacro %}
