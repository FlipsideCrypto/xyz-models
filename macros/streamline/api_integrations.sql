{% macro create_aws_aleo_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %} -- TODO WHEN PROD DEPLOYS
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aleo_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/aleo-api-stg-rolesnowflakeudfsAF733095-JHFljSYqZXeT' api_allowed_prefixes = (
            'https://gvmfebiq6g.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aleo_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/aleo-api-stg-rolesnowflakeudfsAF733095-JHFljSYqZXeT' api_allowed_prefixes = (
            'https://gvmfebiq6g.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
