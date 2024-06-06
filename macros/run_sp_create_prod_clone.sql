{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call m1._internal.create_prod_clone(
        'm1',
        'm1_dev',
        'internal_dev'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
