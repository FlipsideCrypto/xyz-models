{{ config(
    materialized = 'table'
) }}

SELECT
    address,
    'evmos' AS blockchain,
    'flipside' AS creator,
    'operator' AS label_type,
    'validator' AS label_subtype,
    DATA :description :moniker :: STRING AS label,
    DATA :description :identity :: STRING AS project_name,
    DATA :delegator_shares :: INT AS delegator_shares,
    DATA :jailed :: BOOLEAN AS jailed,
    DATA :commission :commission_rates :rate :: FLOAT AS rate,
    DATA :commission :commission_rates :max_change_rate :: FLOAT AS max_change_rate,
    DATA :commission :commission_rates :max_rate :: FLOAT AS max_rate,
    DATA :min_self_delegation :: INT AS min_self_delegation,
    RANK() over (
        PARTITION BY address
        ORDER BY
            DATA :delegator_shares :: INT DESC
    ) AS RANK,
    DATA AS raw_metadata
FROM
    {{ ref('bronze_api__get_validator_metadata_lcd') }}
