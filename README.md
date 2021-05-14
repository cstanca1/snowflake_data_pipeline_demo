# Snowflake Automated and Governed Data Pipeline Demo

Setup: dp_setup.sql

Demo: dp_demo.sql

**Continuous Loading with Snowpipe**

Snowpipe is server-less service with instant scalability to handle variables volume of data, with per second billing that streams data near-real-time to Snowflake. 
It can be used in two ways: 
- Auto-ingest: when files arrive in the stage (S3 bucket, for example), an SQS event notification is created and submitted to an SQS queue, a construct called PIPE will pick up the file and copy its RAW content into Snowflake.
- Via REST API calls; this is an asynchronous process and can be orchestrated, time-based or event-driven. Any popular orchestration tool can be used, for example AirFlow. The PIPE construct will pick up the file and copy its RAW content into Snowflake.

<img src="./snowflake_data_pipe_demo_architecture.png">
