{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

{% if is_incremental() %}
{% set last_version = get_last_transaction_version_created_coin_info() %}
{% endif %}

WITH params AS (

    SELECT
        'query MyQuery { coin_infos( limit: 100 order_by: {transaction_version_created: asc}' ||

{% if is_incremental() %}
'where: {transaction_version_created: {_gte: "' || {{ last_version }} || '"}} ' ||
{% endif %}

') { coin_type coin_type_hash creator_address decimals name symbol transaction_created_timestamp transaction_version_created }} ' AS query
),
res AS (
    SELECT
        live.udf_api(
            'post',
            'https://indexer.mainnet.aptoslabs.com/v1/graphql',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'query',
                query,
                'variables',{}
            )
        ) AS res,
        query,
        SYSDATE() AS _inserted_timestamp
    FROM
        params
)
SELECT
    {# res, #}
    query,
    C.value :coin_type :: STRING AS coin_type,
    C.value :coin_type_hash :: STRING AS coin_type_hash,
    C.value :creator_address :: STRING AS creator_address,
    C.value :decimals :: INT AS decimals,
    C.value :name :: STRING AS NAME,
    C.value :symbol :: STRING AS symbol,
    C.value :transaction_created_timestamp :: datetime AS transaction_created_timestamp,
    C.value :transaction_version_created :: INT AS transaction_version_created,
    _inserted_timestamp
FROM
    res,
    LATERAL FLATTEN(
        input => res :data :data :coin_infos
    ) C
