{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_m1_api_prod AS 'https://ks3xadgzg3.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        aws_m1_api_dev AS 'https://z2wyjkp9r7.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
