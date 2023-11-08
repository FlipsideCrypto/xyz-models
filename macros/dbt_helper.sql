{% macro get_last_transaction_version_created_coin_info() %}
    {% if execute %}
        {% set last_version = run_query("SELECT MAX(transaction_version_created) FROM bronze_api.aptoslabs_coin_info").columns [0] [0] %}
    {% else %}
        {% set last_version = -1 %}
    {% endif %}

    {% do return(last_version) %}
{% endmacro %}
