use datascience_dev._live;

select * from streamline.sportsdb_live_scores;

show api integrations;

desc function datascience_dev._live.udf_api;

desc api integration aws_datascience_api_stg;

drop database datascience_dev;

GRANT OWNERSHIP ON DATABASE datascience_dev TO ROLE INTERNAL_DEV;