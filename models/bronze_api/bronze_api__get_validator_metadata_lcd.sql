{{ config(
    materialized = 'table'
) }}

WITH call AS (

    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://lcd-evmos.keplr.app/cosmos/staking/v1beta1/validators?pagination.limit=5000',{},{}
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
),
keep_last_if_failed AS (
    SELECT
        i.value :operator_address :: STRING AS address,
        i.value AS DATA,
        _inserted_timestamp,
        2 AS RANK
    FROM
        call,
        LATERAL FLATTEN(
            input => resp :data :validators
        ) i
    UNION ALL
    SELECT
        address,
        DATA,
        _inserted_timestamp,
        1 AS RANK
    FROM
        bronze_api.get_validator_metadata_lcd
)
SELECT
    address,
    DATA,
    _inserted_timestamp
FROM
    keep_last_if_failed A
    JOIN (
        SELECT
            MAX(RANK) max_rank
        FROM
            keep_last_if_failed
    ) b
    ON A.rank = b.max_rank