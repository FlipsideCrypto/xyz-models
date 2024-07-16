{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore','full_test']
) }}
{# post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,swapper);", #}
{% if execute %}

{% if is_incremental() %}
{% set query %}
CREATE
OR REPLACE temporary TABLE core.dex_swaps__intermediate_tmp AS

SELECT
    platform,
    MAX(modified_timestamp) modified_timestamp
FROM
    {{ this }}
GROUP BY
    platform {% endset %}
    {% do run_query(
        query
    ) %}
{% endif %}
{% endif %}
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    A.platform,
    event_address,
    swapper,
    token_in,
    token_out,
    amount_in_unadj,
    amount_out_unadj,
    dex_swaps_combined_id AS fact_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__dex_swaps_combined') }} A

{% if is_incremental() %}
LEFT JOIN core.dex_swaps__intermediate_tmp b
ON A.platform = b.platform
WHERE
    A.modified_timestamp >= b.modified_timestamp
    OR b.modified_timestamp IS NULL
{% endif %}
