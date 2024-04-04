poc:
	@dbt run \
		--vars '{"UPDATE_UDFS_AND_SPS": false}' \
		-m "tag:sportsdb" \
		--profile datascience \
		--target dev \
		--profiles-dir ~/.dbt