{% macro run_sp_create_prod_clone() %}
{% set clone_query %}
call evmos._internal.create_prod_clone('evmos', 'evmos_dev', 'internal_dev');
{% endset %}

{% do run_query(clone_query) %}
{% endmacro %}