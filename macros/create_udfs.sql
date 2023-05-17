{% macro create_udfs() %}
    {{ create_udtf_get_base_table(
        schema = "streamline"
    ) }}
    {{ create_udf_get_chainhead() }}
    {{ create_udf_json_rpc() }}
    {{ create_udf_get_tendermint_transactions() }}
    {{ create_udf_get_tendermint_validators() }}
{% endmacro %}
