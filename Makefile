# set default target
DBT_TARGET ?= dev
SOURCE_EVM_DB ?= 'avalanche'
ephemeral:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false, STREAMLINE_INVOKE_STREAMS: false}' \
		-m tag:quantum_poc_ephemeral \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt

bronze:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false, STREAMLINE_INVOKE_STREAMS: true}' \
		-m 2+models/streamline/quantum/poc/core/streamline__aptos_blocks_tx.sql \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt

bronze-layer:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false, STREAMLINE_INVOKE_STREAMS: true}' \
		-m tag:bronze \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt

silver:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false, STREAMLINE_INVOKE_STREAMS: true}' \
		-m 2+tag:silver_blocks \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt

onchain_scores:
	@dbt run \
		-s onchain_scores__near_v1 \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt 

source_evm_privileges:
	@dbt run-operation grant_evm_source_privileges \
		--args '{ "database": $(SOURCE_EVM_DB)}' \
		--profile datascience \
		--target $(DBT_TARGET) \
		--profiles-dir ~/.dbt 