{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

{% set models = [ ('a',ref('silver__nft_sales_bluemove')),('a',ref('silver__nft_sales_bluemove_v2')),('a',ref('silver__nft_sales_mercato')),('a',ref('silver__nft_sales_okx')),('a',ref('silver__nft_sales_seashrine')),('a',ref('silver__nft_sales_souffl3')),('a',ref('silver__nft_sales_topaz')),('a',ref('silver__nft_sales_wapal'))] %}

SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    buyer_address,
    seller_address,
    nft_address,
    token_version,
    platform_address,
    project_name,
    tokenid,
    platform_name,
    platform_exchange_version,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    nft_sales_bluemove_id AS nft_sales_combined_view_id,
    inserted_timestamp,
    modified_timestamp,
    _INSERTED_TIMESTAMP,
    _INVOCATION_ID
FROM
    ({% for models in models %}
    SELECT
        *
    FROM
        {{ models [1] }}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %})
