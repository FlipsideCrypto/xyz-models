{% macro create_aws_aptos_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aptos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/aptos-api-prod-rolesnowflakeudfsAF733095-wEyotLQyFEIl' api_allowed_prefixes = (
            'https://dedvhh9fi1.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aptos_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/aptos-api-dev-rolesnowflakeudfsAF733095-sLREQ0qf4XVH' api_allowed_prefixes = (
            'https://jx15f84555.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
