{% macro create_aws_m1_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_m1_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/m1-api-prod-rolesnowflakeudfsAF733095-CC2Jd3owwPwp' api_allowed_prefixes = (
            'https://ks3xadgzg3.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_m1_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/m1-api-stg-rolesnowflakeudfsAF733095-bEigMvFi81Fd' api_allowed_prefixes = (
            'https://z2wyjkp9r7.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
