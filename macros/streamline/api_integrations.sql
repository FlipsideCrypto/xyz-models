-- macro used to create crosschain sl 2.0 api integrations
{% macro create_aws_datascience_api() %}
    {% if target.name == "prod" %}
        {{ log("Generating api integration for target:" ~ target.name, info=True) }}
        {% set sql %}

        CREATE api integration IF NOT EXISTS aws_datascience_api_prod api_provider = aws_api_gateway api_aws_role_arn = '' api_allowed_prefixes = ('') enabled = TRUE;    
        {% endset %}
        {% do adapter.execute(sql) %}
    {% elif target.name == "dev" %}
        {{ log("Generating stg api integration for target:" ~ target.name, info=True) }}
        {% set sql %}

        CREATE api integration IF NOT EXISTS aws_datascience_api_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/datascience-api-stg-rolesnowflakeudfsAF733095-DeSFZOpWif1k' api_allowed_prefixes = (
            'https://65sji95ax3.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;    
        {% endset %}
        {% do adapter.execute(sql) %}

    {% endif %}
{% endmacro %}




