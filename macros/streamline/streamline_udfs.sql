{% macro create_udf_get_chainhead() %}
    {% if target.name == "prod" %}
        CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_bitcoin_api AS 
            'https://9iyki2rp1b.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_bitcoin_dev_api AS 
            'https://f81vesdos6.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'  
    {%- endif %};
{% endmacro %}

{% macro create_udf_json_rpc() %}
    {% if target.name == "prod" %}
        CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_bitcoin_api AS 
            'https://9iyki2rp1b.execute-api.us-east-1.amazonaws.com/prod/bulk_get_json_rpc'
    {% else %}
        CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_bitcoin_dev_api AS 
            'https://f81vesdos6.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}
