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
		-m tag:onchain_scoress \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt 