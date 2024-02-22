{{ config(
    materialized = 'incremental',
    cluster_by = ["_inserted_timestamp::DATE"],
    unique_key = "tx_id",
    tags = ["load", "scheduled_core"],
    incremental_strategy = 'delete+insert',
    incremental_predicates = ['block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH streamline_transactions AS (

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
FINAL AS (
    SELECT
        block_number,
        DATA,
        _inserted_timestamp,
        id,
        _partition_by_block_id,
        VALUE,
        DATA :txid :: STRING AS tx_id
    FROM
        streamline_transactions
)
SELECT
    *
FROM
    FINAL
WHERE
    TYPEOF(
        DATA :: variant
    ) != 'ARRAY' 
qualify ROW_NUMBER() over (
        PARTITION BY tx_id,
        block_number
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
