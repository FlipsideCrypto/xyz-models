{% macro create_api_integration(project_name, snowflake_role_arn, endpoint_urls) %}
  {% set integration_name = "aws_" ~ project_name ~ "_api" %}
  {% set allowed_prefixes = [] %}
  {% for url in endpoint_urls %}
    {% do allowed_prefixes.append("'" ~ url ~ "'") %}
  {% endfor %}
  {% set allowed_prefixes = allowed_prefixes|join(", ") %}
  {% set sql %}
    CREATE OR REPLACE API INTEGRATION {{ integration_name }}
    API_PROVIDER = aws_api_gateway 
    API_AWS_ROLE_ARN = '{{ snowflake_role_arn }}' 
    API_ALLOWED_PREFIXES = ({{ allowed_prefixes }})
    ENABLED=true
  {% endset %}

  {% do log(sql, info=true)%}

  {% do run_query(sql) %}
  {% do log("API Integration " ~ integration_name ~ " successfully created", true) %}
{% endmacro %}