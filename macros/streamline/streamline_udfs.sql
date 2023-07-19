{% macro create_udf_get_chainhead() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_aurora_api AS 
            'https://sl2f5beopl.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_aurora_dev_api AS 
            'https://66lx4fxkui.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'  
    {%- endif %};
{% endmacro %}

{% macro create_udf_json_rpc() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_aurora_api AS 
            'https://sl2f5beopl.execute-api.us-east-1.amazonaws.com/prod/bulk_get_json_rpc'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_aurora_dev_api AS 
            'https://66lx4fxkui.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}