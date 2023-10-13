{% macro run_create_sum_vout_values() %}
{% set sql %}
create or replace function {{ target.database }}.SILVER.UDF_SUM_VOUT_VALUES(vout ARRAY) 
returns float
language python
runtime_version = '3.9'
handler = 'sum_vout_values'
AS
$$
def sum_vout_values(vout):
    return sum(output['value'] for output in vout)
$$
;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
