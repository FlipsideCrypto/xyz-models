{% macro create_api_integration(project_name, snowflake_role_arn, endpoint_urls) %}
  {% set integration_name = "aws_" ~ project_name ~ "_api" %}
  {% set allowed_prefixes = "(" ~ endpoint_urls|join(", ") ~ ")" %}
  {% set sql %}
    CREATE API INTEGRATION {{ integration_name }}
    API_PROVIDER = aws_api_gateway 
    API_AWS_ROLE_ARN = '{{ snowflake_role_arn }}' 
    API_ALLOWED_PREFIXES = {{ allowed_prefixes }}
  {% endset %}
  {% do run_query(sql) %}
  {% do log("API Integration " ~ integration_name ~ " successfully created", true) %}
{% endmacro %}