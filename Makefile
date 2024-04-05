quantum-poc:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false, STREAMLINE_INVOKE_STREAMS: true}' \
		-m "tag:sportsdb" \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt