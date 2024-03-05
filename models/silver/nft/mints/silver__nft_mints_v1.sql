{{ config(
    materialized = 'incremental',
    unique_key = "nft_mints_v1_id",
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
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        success
        AND event_type IN (
            '0x3::token::MintTokenEvent',
            '0x3::token::DepositEvent',
            '0x1::coin::WithdrawEvent'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
v1_mint_events_raw AS (
    SELECT
        *,
        COUNT(tx_hash) over (
            PARTITION BY tx_hash
            ORDER BY
                tx_hash
        ) AS mint_count,
        'v1' AS token_version,
        LAG(
            event_index,
            1,
            0
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS prev_event_index,
        LEAD(
            event_index,
            1,
            0
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS next_event_index_raw
    FROM
        evnts
    WHERE
        event_type = '0x3::token::MintTokenEvent'
),
deposit_v1_token_tx AS (
    SELECT
        *
    FROM
        evnts
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                v1_mint_events_raw
        )
        AND event_type = '0x3::token::DepositEvent'
),
coin_withdraw_events_v2 AS (
    SELECT
        *,
        MIN(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS min_with_index,
        MAX(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index DESC
        ) AS max_with_index
    FROM
        evnts
    WHERE
        event_type = '0x1::coin::WithdrawEvent'
),
v1_mints_with_nft_address AS (
    SELECT
        *
    FROM
        (
            SELECT
                mint.*,
                deposit.event_data :amount :: NUMBER AS nft_count,
                deposit.event_data AS deposit_data,
                deposit.event_data :id.property_version :: NUMBER AS property_version,
                deposit.event_data :id.token_data_id.creator || '::' || deposit.event_data :id.token_data_id.collection || '::' || deposit.event_data :id.token_data_id.name || '::' || deposit.event_data :id.property_version AS nft_address
            FROM
                v1_mint_events_raw mint
                JOIN deposit_v1_token_tx deposit
                ON mint.tx_hash = deposit.tx_hash
                AND mint.event_data :id.name = deposit.event_data :id.token_data_id.name
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                property_version DESC
        ) = 1
),
v1_mints_with_token_info AS (
    SELECT
        *,
        MIN(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS min_mint_index,
        event_data :id.collection AS project_name,
        event_data :id.name AS tokenid
    FROM
        v1_mints_with_nft_address
),
v1_mints_with_withdrawals AS (
    SELECT
        *,
        (
            CASE
                WHEN next_event_index_raw = 0 THEN max_with_index
                ELSE next_event_index_raw
            END
        ) AS next_event_index
    FROM
        (
            SELECT
                main.*,
                withdraw.max_with_index
            FROM
                v1_mints_with_token_info main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
            GROUP BY
                ALL
        )
),
v1_mints_with_price_raw1 AS (
    SELECT
        * exclude (
            next_event_index,
            max_with_index
        )
    FROM
        (
            SELECT
                main.*,
                SUM(
                    withdraw.event_data :amount :: NUMBER
                ) over (
                    PARTITION BY main.tx_hash,
                    main.event_index
                    ORDER BY
                        withdraw.event_index
                ) AS price_raw
            FROM
                v1_mints_with_withdrawals main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                AND main.event_index < withdraw.event_index
                AND main.next_event_index >= withdraw.event_index
                AND main.min_mint_index < withdraw.min_with_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                price_raw DESC
        ) = 1
),
v1_mints_with_price_raw2 AS (
    SELECT
        * exclude (
            next_event_index,
            max_with_index
        )
    FROM
        (
            SELECT
                main.*,
                SUM(
                    withdraw.event_data :amount :: NUMBER
                ) over (
                    PARTITION BY main.tx_hash,
                    main.event_index
                    ORDER BY
                        withdraw.event_index
                ) AS price_raw
            FROM
                v1_mints_with_withdrawals main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                AND main.event_index > withdraw.event_index
                AND main.prev_event_index <= withdraw.event_index
                AND main.min_mint_index > withdraw.min_with_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                price_raw DESC
        ) = 1
),
combined_raw1_raw2 AS (
    SELECT
        *
    FROM
        v1_mints_with_price_raw1
    UNION
    SELECT
        *
    FROM
        v1_mints_with_price_raw2
),
with_tx_count AS (
    SELECT
        tx_hash,
        COUNT(tx_hash) AS with_tx_count
    FROM
        v1_mints_with_withdrawals
    GROUP BY
        tx_hash
),
comb_tx_count AS (
    SELECT
        tx_hash,
        COUNT(tx_hash) AS comb_tx_count
    FROM
        combined_raw1_raw2
    GROUP BY
        tx_hash
),
tx_count_full AS (
    SELECT
        withd.*,
        comb.comb_tx_count
    FROM
        with_tx_count withd
        JOIN comb_tx_count comb
        ON withd.tx_hash = comb.tx_hash
),
v1_shared_mint_cost AS (
    SELECT
        * exclude (
            max_with_index,
            next_event_index,
            total_amount
        )
    FROM
        (
            SELECT
                main.*,
                SUM(
                    withdraw.event_data :amount
                ) over (
                    PARTITION BY withdraw.tx_hash
                    ORDER BY
                        withdraw.tx_hash
                ) AS total_amount,
                total_amount :: NUMBER / main.mint_count AS price_raw
            FROM
                v1_mints_with_withdrawals main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
            WHERE
                main.tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        tx_count_full
                    WHERE
                        with_tx_count != comb_tx_count
                )
        )
    GROUP BY
        ALL
),
fin AS (
    SELECT
        *
    FROM
        v1_shared_mint_cost
    UNION ALL
    SELECT
        *
    FROM
        combined_raw1_raw2
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v1_shared_mint_cost
        )
    UNION ALL
    SELECT
        *,
        0 AS price_raw
    FROM
        v1_mints_with_token_info
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                v1_mints_with_withdrawals
        )
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_resource AS event_type,
    nft_address :: STRING AS nft_address,
    project_name :: STRING AS project_name,
    event_address AS nft_from_address,
    account_address AS nft_to_address,
    tokenid :: STRING AS tokenid,
    token_version,
    nft_count,
    price_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_mints_v1_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
