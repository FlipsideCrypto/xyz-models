{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

{% set models = [
    ('animeswap', ref('silver__dex_swaps_animeswap')),
    ('auxexchange', ref('silver__dex_swaps_auxexchange')),
    ('batswap', ref('silver__dex_swaps_batswap')),
    ('cellana', ref('silver__dex_swaps_cellana')),
    ('cetus', ref('silver__dex_swaps_cetus')),
    ('hippo', ref('silver__dex_swaps_hippo')),
    ('liquidswap', ref('silver__dex_swaps_liquidswap')),
    ('pancake', ref('silver__dex_swaps_pancake')),
    ('sushi', ref('silver__dex_swaps_sushi')),
    ('thala', ref('silver__dex_swaps_thala')),
    ('thala', ref('silver__dex_swaps_thala_v0')),
    ('tsunami', ref('silver__dex_swaps_tsunami'))

]
 %}
     {# ,    ('aires', ref('silver__dex_swaps_aires')) #}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    platform,
    event_address,
    swapper,
    token_in,
    token_out,
    amount_in_unadj,
    amount_out_unadj,
    dex_swaps_animeswap_id AS dex_swaps_combined_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    ({% for models in models %}
    SELECT
        '{{ models[0] }}' AS platform,*
    FROM
        {{ models [1] }}

        {% if not loop.last %}

{% if is_incremental() %}
{% endif %}
UNION ALL
{% endif %}
{% endfor %})
