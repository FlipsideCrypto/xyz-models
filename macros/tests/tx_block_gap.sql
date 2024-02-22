-- tests if any blocks did not make it into the transactions model, which indicate a processing issue
{% test tx_block_gaps(
    model,
    column_name
) %}
SELECT
    b.{{ column_name }} AS block_number_b,
    t.{{ column_name }} AS block_number_t
FROM
    {{ ref('silver__blocks') }}
    b
    LEFT JOIN {{ model }}
    t
    ON b.{{ column_name }} = t.{{ column_name }}
WHERE
    _inserted_timestamp <= SYSDATE() - INTERVAL '2 hours'
    AND block_number_t IS NULL 
{% endtest %}
