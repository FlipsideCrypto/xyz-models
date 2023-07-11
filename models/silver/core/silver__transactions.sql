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
final AS (
    SELECT
        block_number,
        DATA :hash :: STRING AS tx_hash,
        DATA :hex :: STRING AS hex,
        DATA :locktime :: STRING AS lock_time,
        DATA :size :: NUMBER AS SIZE,
        DATA :txid :: STRING AS tx_id,
        DATA :version :: NUMBER AS version,
        DATA :vin :: ARRAY AS inputs,
        array_size(inputs) as input_count,
        DATA :vout :: ARRAY AS outputs,
        array_size(outputs) as output_count,
        DATA :vsize :: STRING AS virtual_size,
        DATA :weight :: STRING AS weight,
        DATA: fee :: FLOAT as fee,
        _partition_by_block_id,
        _inserted_timestamp,
        DATA
    FROM
        bronze_transactions
)
-- TODO input value and output value
-- will need flattened inputs/outputs so do that in those models and join agg value in 
-- tx index?? possible from blocks array order
SELECT
    *
FROM
    final
