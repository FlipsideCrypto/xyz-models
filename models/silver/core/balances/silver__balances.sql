{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','change_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core','full_test'],
    enabled = false
) }}
{# cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, change_type,inner_change_type,change_address,change_module,change_resource);",
#}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    REPLACE(
        REPLACE(
            change_resource :: STRING,
            'CoinStore<'
        ),
        '>'
    ) AS token_address,
    change_data :coin :value :: INT AS post_balance,
    COALESCE(
        change_data :deposit_events :guid :id :addr,
        change_data :withdraw_events :guid :id :addr,
        change_data :coin_amount_event :guid :id :addr
    ) :: STRING AS account_address,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','version','account_address','token_address']
    ) }} AS changes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'silver__changes'
    ) }}
WHERE
    post_balance IS NOT NULL
