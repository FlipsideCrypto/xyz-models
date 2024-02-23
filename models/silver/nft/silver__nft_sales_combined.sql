{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_combined_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH all_nft_platform_sales AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_sales_combined_view') }}
),
txns AS (
    SELECT
        tx_hash,
        payload_function
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
xfers AS (
    SELECT
        tx_hash,
        account_address,
        token_address,
        event_index,
        transfer_event
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
aggregator_nft_sales AS (
    SELECT
        tx_hash,
        CASE
            WHEN payload_function ILIKE '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26%' THEN 'Mercato'
            WHEN payload_function ILIKE '0xb339d393479e88d35ebf440f230c3d47ffa87f81012eb29ba8a4a3b2c689eda9%' THEN 'Mercato'
            WHEN payload_function ILIKE '0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4%' THEN 'Souffl3'
        END AS aggregator_name
    FROM
        txns
    WHERE
        payload_function IN (
            '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26::markets_v2::buy_tokens_v2',
            '0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4::Aggregator::batch_buy_script_V1',
            '0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4::Aggregator::batch_buy_script_V2',
            '0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4::Aggregator::batch_buy_script_V3',
            '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26::biddings::unlist_and_accept_token_bid_v2',
            '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26::markets::buy_tokens',
            '0xb339d393479e88d35ebf440f230c3d47ffa87f81012eb29ba8a4a3b2c689eda9::markets_v2::buy_tokens_v2',
            '0xb339d393479e88d35ebf440f230c3d47ffa87f81012eb29ba8a4a3b2c689eda9::markets_v2::buy_tokens_v3'
        )
),
all_nft_platform_sales_with_agg AS (
    SELECT
        main.*,
        agg.aggregator_name
    FROM
        all_nft_platform_sales main
        LEFT JOIN aggregator_nft_sales agg
        ON main.tx_hash = agg.tx_hash
        AND main.platform_name != agg.aggregator_name
    GROUP BY
        ALL
),
associated_transfers AS (
    SELECT
        A.tx_hash,
        A.account_address,
        A.token_address
    FROM
        xfers A
        JOIN all_nft_platform_sales_with_agg b
        ON A.tx_hash = b.tx_hash
        AND A.account_address = b.seller_address
    WHERE
        transfer_event = 'DepositEvent' qualify (ROW_NUMBER() over (PARTITION BY A.tx_hash, A.account_address
    ORDER BY
        A.event_index DESC)) = 1
)
SELECT
    block_timestamp,
    block_number,
    version,
    main.tx_hash,
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
    total_price_raw :: INT AS total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    aggregator_name,
    xfers.token_address AS currency_address,
    nft_sales_combined_view_id AS nft_sales_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_nft_platform_sales_with_agg main
    JOIN associated_transfers xfers
    ON main.tx_hash = xfers.tx_hash
    AND main.seller_address = xfers.account_address
