{{ config(
    materialized = 'view'
) }}

SELECT
    address,
    blockchain,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    delegator_shares,
    jailed,
    rate,
    max_change_rate,
    max_rate,
    min_self_delegation,
    RANK,
    raw_metadata,
    unique_key
FROM
    {{ ref('silver__validators') }}
