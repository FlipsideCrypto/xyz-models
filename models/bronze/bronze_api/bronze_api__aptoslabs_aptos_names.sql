{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['noncore']
) }}

{% if is_incremental() %}
{% set last_version = get_last_transaction_version_aptos_names() %}
{% endif %}

WITH params AS (

    SELECT
        'query MyQuery { current_aptos_names( limit: 100 order_by: {last_transaction_version: asc}' ||

{% if is_incremental() %}
'where: {last_transaction_version: {_gte: "' || {{ last_version }} || '"}} ' ||
{% endif %}

' ) { domain domain_with_suffix expiration_timestamp is_active is_primary last_transaction_version owner_address registered_address subdomain token_name token_standard } } ' AS query
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
    C.value :domain :: STRING AS domain,
    C.value :domain_with_suffix :: STRING AS domain_with_suffix,
    C.value :expiration_timestamp :: datetime AS expiration_timestamp,
    C.value :is_active :: BOOLEAN AS is_active,
    C.value :is_primary :: BOOLEAN AS is_primary,
    C.value :last_transaction_version :: INT AS last_transaction_version,
    C.value :owner_address :: STRING AS owner_address,
    C.value :registered_address :: STRING AS registered_address,
    C.value :subdomain :: STRING AS subdomain,
    C.value :token_name :: STRING AS token_name,
    C.value :token_standard :: STRING AS token_standard,
    _inserted_timestamp
FROM
    res,
    LATERAL FLATTEN(
        input => res :data :data :current_aptos_names
    ) C
