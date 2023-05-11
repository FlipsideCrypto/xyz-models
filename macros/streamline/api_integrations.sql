{% macro create_aws_ethereum_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_evmos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-evmos' api_allowed_prefixes = (
            'https://55h4rahr50.execute-api.us-east-1.amazonaws.com/dev/',
            'https://n0reh6ugbf.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
