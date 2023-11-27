{{ config(
    materialized = 'view',
    tags = ['atlas']
) }}

SELECT
    atlas_maa_id AS fact_maas_id,
    day,
    maa,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__atlas_maa') }}
