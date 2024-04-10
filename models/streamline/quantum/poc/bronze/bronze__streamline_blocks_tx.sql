{{ config (
    materialized = 'view',
    tags = ['bronze']
) }}

{% set model = this.identifier.split("_") [-1] %}
{{ streamline_external_table_query(
    model = "quantum_poc_aptos_blocks_tx",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )",
    partition_name = "_partition_by_block_id",
    unique_key = "partition_key"
) }}
