{% macro create_udf_hex_to_int(schema) %}
create or replace function {{ schema }}.udf_hex_to_int(hex string)
returns string
language python
runtime_version = '3.8'
handler = 'hex_to_int'
as
$$
def hex_to_int(hex) -> str:
  """
  Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
  select hex_to_int('200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int('0x200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int(NULL);
  >> NULL
  """
  return str(int(hex, 16)) if hex else None
$$;
{% endmacro %}

{% macro create_udf_decode_hex_to_string() %}
CREATE OR REPLACE FUNCTION {{ target.database }}.STREAMLINE.UDF_DECODE_HEX_TO_STRING(hex_string STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER = 'decode_hex_to_string'
AS
$$
import binascii

def decode_hex_to_string(hex_string):
    """
    This UDF decodes a hexadecimal string input into its original ASCII representation using the binascii.unhexlify function in Python. 
    If successful, it returns the decoded message; otherwise, it handles errors and returns the error message or NULL if decoding fails.
    The primary use-case for this function will be to decode the COINBASE message.
    """
    try:
        decoded_message = binascii.unhexlify(hex_string).decode("utf-8", errors="ignore")
        return decoded_message
    except Exception as e:
        return str(e)

$$;
{% endmacro %}
