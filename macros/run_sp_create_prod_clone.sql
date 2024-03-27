{% macro run_sp_create_prod_clone() %}
{% set clone_query %}
call datascience._internal.create_prod_clone('datascience', 'datascience_dev', 'internal_dev');
{% endset %}

{% do run_query(clone_query) %}
{% endmacro %}