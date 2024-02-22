{{ config(
    materialized = 'incremental',
    cluster_by = ["_inserted_timestamp::DATE"],
    unique_key = 'block_number',
    tags = ["load", "scheduled_core"],
    incremental_strategy = 'delete+insert'
) }}
-- depends on {{ref('bronze__streamline_blocks')}}
WITH streamline_blocks AS (

    SELECT
        *
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        DATA,
        _inserted_timestamp,
        id,
        _partition_by_block_id,
        VALUE
    FROM
        streamline_blocks
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY block_number
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
