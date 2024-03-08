{% macro create_aws_bitcoin_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_bitcoin_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/bitcoin-api-prod-rolesnowflakeudfsAF733095-BXQYHR6NEDUG' api_allowed_prefixes = (
            'https://xgp8ztpp0b.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_bitcoin_dev_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/bitcoin-api-dev-rolesnowflakeudfsAF733095-15K9O939DPMBA' api_allowed_prefixes = (
            'https://f81vesdos6.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
