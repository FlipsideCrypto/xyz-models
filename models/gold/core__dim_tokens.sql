{{ config(
    materialized = 'table'
) }}

SELECT
    'evmos' AS blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    alias,
    DECIMAL,
    raw_metadata, 
    concat_ws(
        '-',
        address,
        creator,
        blockchain
    ) AS unique_key
FROM
    {{ source(
        'osmo',
        'asset_metadata'
    ) }}
