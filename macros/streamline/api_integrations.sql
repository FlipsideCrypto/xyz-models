{% macro create_aws_aurora_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aurora_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/aurora-api-prod-rolesnowflakeudfsAF733095-3WVDCVO54NPX' api_allowed_prefixes = (
            'https://sl2f5beopl.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aurora_dev_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/aurora-api-dev-rolesnowflakeudfsAF733095-1MX4LOX4UFE9M' api_allowed_prefixes = (
            'https://66lx4fxkui.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}