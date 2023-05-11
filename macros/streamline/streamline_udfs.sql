{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_evmos_api AS {% if target.name == "prod" %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {% else %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_json_rpc(
        json OBJECT
    ) returns ARRAY api_integration = aws_evmos_api AS {% if target.name == "prod" %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {% else %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_tendermint_transactions() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.bulk_get_tendermint_transactions(
        json OBJECT
    ) returns ARRAY api_integration = aws_evmos_api AS {% if target.name == "prod" %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_tendermint_transactions'
    {% else %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_tendermint_transactions'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_tendermint_validators() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.bulk_get_tendermint_validators(
        json OBJECT
    ) returns ARRAY api_integration = aws_evmos_api AS {% if target.name == "prod" %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_tendermint_validators'
    {% else %}
        'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/bulk_get_tendermint_validators'
    {%- endif %};
{% endmacro %}