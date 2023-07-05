{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ["_inserted_timestamp::DATE", "block_number"]
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH bronze_transactions AS (

    SELECT
        *
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
),
transactions AS (
    SELECT
        block_number,
        DATA :hash :: STRING AS tx_hash,
        DATA :hex :: STRING AS hex,
        DATA :locktime :: STRING AS locktime,
        DATA :size :: NUMBER AS SIZE,
        DATA :txid :: STRING AS tx_id,
        DATA :version :: NUMBER AS version,
        DATA :vin :: ARRAY AS vector_input,
        DATA :vout :: ARRAY AS vector_ouput,
        DATA :vsize :: STRING AS virtual_size,
        DATA :weight :: STRING AS weight,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        bronze_transactions
)
SELECT
    *
FROM
    transactions
