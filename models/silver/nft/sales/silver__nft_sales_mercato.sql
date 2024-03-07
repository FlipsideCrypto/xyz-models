{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_mercato_id",
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
            WHEN event_resource = 'BuyEvent' THEN 'sale'
            WHEN event_resource IN (
                'AcceptTokenBidEvent',
                'AcceptCollectionBidEvent'
            ) THEN 'bid_won'
        END AS event_kind,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            (
                event_address = '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26' --types of events that indicate a sale
                AND event_resource IN (
                    'BuyEvent',
                    'AcceptTokenBidEvent',
                    'AcceptCollectionBidEvent'
                )
            )
            OR (
                event_resource = 'DepositEvent'
                AND event_module = 'coin'
            )
        )
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
evnts_2 AS (
    SELECT
        *
    FROM
        (
            SELECT
                *,
                LAG(
                    event_index,
                    1,
                    0
                ) over (
                    PARTITION BY tx_hash
                    ORDER BY
                        event_index
                ) AS prev_event_index
            FROM
                (
                    SELECT
                        *,
                        len(event_type) AS event_type_len
                    FROM
                        evnts
                    WHERE
                        (
                            event_address = '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26' --types of events that indicate a sale
                            AND event_resource IN (
                                'BuyEvent',
                                'AcceptTokenBidEvent',
                                'AcceptCollectionBidEvent'
                            )
                        )
                )
            WHERE
                event_type_len > 70
        )
),
txs AS (
    SELECT
        A.block_timestamp,
        A.tx_hash,
        A.payload
    FROM
        {{ ref('silver__transactions') }} A
        JOIN (
            SELECT
                tx_hash,
                block_timestamp :: DATE bd
            FROM
                evnts
            GROUP BY
                tx_hash,
                bd
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.block_timestamp :: DATE = b.bd
    WHERE
        success

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
),
chngs AS (
    SELECT
        A.block_timestamp,
        A.tx_hash,
        A.change_data,
        address
    FROM
        {{ ref('silver__changes') }} A
        JOIN (
            SELECT
                tx_hash,
                block_timestamp :: DATE bd
            FROM
                evnts
            GROUP BY
                tx_hash,
                bd
        ) b
        ON A.tx_hash = b.tx_hash
        AND A.block_timestamp :: DATE = b.bd
    WHERE
        success
        AND inner_change_type = '0x4::token::Token'

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
),
null_col_names AS (
    SELECT
        tx_hash
    FROM
        evnts_2
    WHERE
        event_data :token_id.token_data_id.collection IS NULL
    GROUP BY
        1
),
collection_names AS (
    SELECT
        tx.tx_hash,
        to_varchar(
            f.value
        ) AS collection_name,
        f.index
    FROM
        txs tx
        JOIN null_col_names n
        ON tx.tx_hash = n.tx_hash
        JOIN LATERAL FLATTEN(
            input => payload :arguments [5]
        ) f
),
object_addresses AS (
    SELECT
        tx.tx_hash,
        to_varchar(
            f.value
        ) AS object_address,
        f.index
    FROM
        txs tx
        JOIN null_col_names n
        ON tx.tx_hash = n.tx_hash
        JOIN LATERAL FLATTEN(
            input => payload :arguments [6]
        ) f
),
object_collection_info AS (
    SELECT
        obj.tx_hash,
        coll.collection_name,
        obj.object_address
    FROM
        object_addresses obj
        LEFT JOIN collection_names coll
        ON obj.tx_hash = coll.tx_hash
        AND obj.index = coll.index
),
collection_nft_info AS (
    SELECT
        coll.tx_hash,
        coll.collection_name,
        coll.object_address,
        change.change_data :name AS nft_name
    FROM
        object_collection_info coll
        LEFT JOIN chngs change
        ON coll.tx_hash = change.tx_hash
        AND coll.object_address = change.address
),
missed_object_names AS (
    SELECT
        A.tx_hash,
        change_data :name AS nft_name
    FROM
        chngs A
        JOIN null_col_names n
        ON A.tx_hash = n.tx_hash
        LEFT JOIN collection_nft_info b
        ON A.tx_hash = b.tx_hash
    WHERE
        b.tx_hash IS NULL
),
mercato_bids_sales_events2 AS (
    SELECT
        main.block_timestamp,
        main.block_number,
        main.version,
        main.tx_hash,
        main.event_index,
        main.event_data,
        main.event_address,
        main.payload_function,
        main.event_module,
        main.event_kind,
        main._inserted_timestamp,
        main.prev_event_index,
        (
            CASE
                WHEN ARRAY_SIZE(SPLIT(change.nft_name, ' #')) > 1 THEN SPLIT(
                    change.nft_name,
                    ' #'
                ) [0] :: text
                WHEN SPLIT(
                    change.nft_name,
                    '.'
                ) [2] = 'apt' THEN 'Aptos Subdomain Names V2'
                WHEN SPLIT(
                    change.nft_name,
                    ' '
                ) [4] = '2023' THEN 'Aptos ONE Mainnet Anniversary 2023'
                WHEN ARRAY_SIZE(SPLIT(change.nft_name, '_')) > 1 THEN 'Ordinals Sub 1k Text'
                ELSE change.nft_name :: VARCHAR
            END
        ) :: variant AS collection_name,
        change.nft_name AS nft_name
    FROM
        evnts_2 main
        JOIN missed_object_names change
        ON main.tx_hash = change.tx_hash
),
mercato_bids_sales_events AS (
    SELECT
        main.block_timestamp,
        main.block_number,
        main.version,
        main.tx_hash,
        main.event_index,
        main.event_data,
        main.event_address,
        main.payload_function,
        main.event_module,
        main.event_kind,
        main._inserted_timestamp,
        main.prev_event_index,
        obj.collection_name :: variant AS collection_name,
        obj.nft_name
    FROM
        evnts_2 main
        LEFT JOIN collection_nft_info obj
        ON main.tx_hash = obj.tx_hash
        AND main.event_data :token.inner = obj.object_address
    WHERE
        main.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                mercato_bids_sales_events2
        )
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        version,
        tx_hash,
        event_index,
        event_data,
        event_address,
        payload_function,
        event_module,
        event_kind,
        _inserted_timestamp,
        prev_event_index,
        collection_name,
        nft_name
    FROM
        mercato_bids_sales_events2
),
main_part_mercato AS (
    SELECT
        block_timestamp,
        block_number,
        version,
        tx_hash,
        event_index,
        prev_event_index,
        event_kind AS event_type,
        COALESCE(
            event_data :buyer,
            event_data :bid_buyer
        ) AS buyer_address,
        COALESCE(
            event_data :seller,
            event_data :owner,
            event_data :bid_seller
        ) AS seller_address,
        COALESCE(
            event_data :token_id.token_data_id.creator || '::' || event_data :token_id.token_data_id.collection || '::' || event_data :token_id.token_data_id.name || '::' || event_data :token_id.property_version,
            event_data :token.inner
        ) AS nft_address,
        (
            CASE
                WHEN len(nft_address) > 70 THEN 'v1'
                ELSE 'v2'
            END
        ) AS token_version,
        event_address AS platform_address,
        COALESCE(
            event_data :token_id.token_data_id.collection,
            collection_name
        ) AS project_name,
        COALESCE(
            event_data :token_id.token_data_id.name,
            nft_name
        ) AS tokenid,
        'Mercato' AS platform_name,
        (
            CASE
                WHEN ARRAY_SIZE(SPLIT(event_module, '_')) > 1 THEN 'tradeport_' || SPLIT(
                    event_module,
                    '_'
                ) [1]
                ELSE 'tradeport'
            END
        ) AS platform_exchange_version,
        event_data :price :: NUMBER AS price,
        _inserted_timestamp
    FROM
        mercato_bids_sales_events
),
deposit_events_mercator AS (
    SELECT
        A.tx_hash,
        event_index,
        event_data :amount :: NUMBER AS fee,
        account_address
    FROM
        evnts A
        JOIN (
            SELECT
                tx_hash
            FROM
                main_part_mercato
            GROUP BY
                1
        ) b
        ON A.tx_hash = b.tx_hash
    WHERE
        event_resource = 'DepositEvent'
        AND event_module = 'coin'
),
main_with_creator_fees_raw AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                creator.fee,
                creator.event_index AS creator_ev_index,
                creator.account_address AS creator_royalties_address
            FROM
                main_part_mercato main
                JOIN deposit_events_mercator creator
                ON main.tx_hash = creator.tx_hash
                AND main.event_index > creator.event_index
                AND main.prev_event_index <= creator.event_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                creator_ev_index
        ) = 1
),
main_with_creator_fees AS (
    SELECT
        * exclude (
            fee,
            creator_ev_index,
            creator_royalties_address
        )
    FROM
        (
            SELECT
                *,
                (
                    CASE
                        WHEN creator_royalties_address = '0x6a03eb973cd9385d62fc2842d02a4dd6b70e52f5da77a0689e57e48d93fae1b4' THEN 0
                        ELSE fee
                    END
                ) AS creator_fee_raw
            FROM
                main_with_creator_fees_raw
        )
)
SELECT
    block_timestamp,
    block_number,
    version,
    main.tx_hash,
    main.event_index,
    event_type,
    seller_address :: STRING AS seller_address,
    buyer_address :: STRING AS buyer_address,
    nft_address,
    token_version,
    platform_address,
    project_name :: STRING AS project_name,
    tokenid :: STRING AS tokenid,
    platform_name,
    platform_exchange_version,
    price AS total_price_raw,
    creator_fee_raw,
    platform.fee AS platform_fee_raw,
    creator_fee_raw + platform_fee_raw AS total_fees_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['main.tx_hash','main.event_index']
    ) }} AS nft_sales_mercato_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    main._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    main_with_creator_fees main
    JOIN deposit_events_mercator platform
    ON main.tx_hash = platform.tx_hash
    AND main.event_index > platform.event_index
    AND main.prev_event_index <= platform.event_index
    AND platform.account_address IN (
        '0x6a03eb973cd9385d62fc2842d02a4dd6b70e52f5da77a0689e57e48d93fae1b4',
        '0x41699a1297fba9645eae628d909966659d2da5a425911c3d7bccd54ffce6606a',
        '0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26'
    )
