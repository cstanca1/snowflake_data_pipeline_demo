# Snowflake Automated and Governed Data Pipeline Demo

1. Setup: dp_setup.sql
2. Demo: dp_demo.sql




## Continuous Loading with Snowpipe

Snowpipe is server-less service with instant scalability to handle variables volume of data, with per second billing that streams data near-real-time to Snowflake. 

Snowpipe can be used in two ways: 
- Auto-ingest: when files arrive in the stage (S3 bucket, for example), an SQS event notification is created and submitted to an SQS queue, a construct called PIPE will pick up the file and copy its RAW content into Snowflake.
- Via REST API calls; this is an asynchronous process and can be orchestrated, time-based or event-driven. Any popular orchestration tool can be used, for example AirFlow. The PIPE construct will pick up the file and copy its RAW content into Snowflake.

The demo covers the auto-ingest option.

This demo data pipeline leverages also other powerful features, e.g. streams, tasks, stored procedures, secure dynamic views, secure data sharing.

<img src="./snowflake_data_pipe_demo_architecture.png">
