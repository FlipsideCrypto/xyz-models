{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_bluemove_v2_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH evnts AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        account_address,
        event_address,
        event_resource,
        event_data,
        event_module,
        event_type,
        CASE
            WHEN event_resource = 'ListingFilledEvent' THEN 'sale'
            WHEN event_resource IN (
                'TokenOfferFilledEvent',
                'CollectionOfferFilledEvent'
            ) THEN 'bid_won'
        END AS event_kind,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_address = '0xd520d8669b0a3de23119898dcdff3e0a27910db247663646ad18cf16e44c6f5'
        AND event_resource IN (
            'ListingFilledEvent',
            'TokenOfferFilledEvent',
            'CollectionOfferFilledEvent'
        )
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    {# AND block_timestamp :: DATE >= '2022-10-19' #}
{% endif %}
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_kind AS event_type,
    event_data :purchaser :: STRING AS buyer_address,
    event_data :seller :: STRING AS seller_address,
    COALESCE(
        event_data :id.token_data_id.creator || '::' || event_data :id.token_data_id.collection || '::' || event_data :id.token_data_id.name || '::' || event_data :id.property_version,
        event_data :token_metadata.token.vec [0].inner,
        event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version
    ) AS nft_address,
    (
        CASE
            WHEN len(nft_address) > 70 THEN 'v1'
            ELSE 'v2'
        END
    ) AS token_version,
    event_address AS platform_address,
    event_data :token_metadata.collection_name :: STRING AS project_name,
    event_data :token_metadata.token_name :: STRING AS tokenid,
    'BlueMove' AS platform_name,
    'bluemove_marketplace_token_v2' AS platform_exchange_version,
    event_data :price :: NUMBER AS total_price_raw,
    event_data :commission :: NUMBER AS platform_fee_raw,
    event_data :royalties :: NUMBER AS creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_sales_bluemove_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts
