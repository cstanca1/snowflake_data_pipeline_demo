-- PRE_REQS:
-- storage integration and stage setup done in dp_setup script (one-time setup) - also function/SP utils
-- use storage integration integration YOURSTORAGEINTEGRATION
-- use stage snowflake_demo_stage
-- create all objects needed: pipe, tables, streams, tasks

-- set context
use role accountadmin;
use database snowflake_demo;
use warehouse datapipeline_wh; -- it assumes that you have it created

-- clean-up before demo
rm @snowflake_demo.public.snowflake_demo_stage;
-- truncate tables in case that they existed and some records were there from the last demo
-- if any records, truncate
truncate table if exists snowflake_demo.raw.trips_raw;
truncate table if exists snowflake_demo.modelled.trips;
truncate table if exists snowflake_demo.modelled.stations;
truncate table if exists snowflake_demo.modelled.programs;


/*--------------------------------------------------------------------------------
  DATA PIPELINE: PROVIDER -> SNOWPIPE -> STREAMS -> TASKS -> SECURE DATA SHARING
--------------------------------------------------------------------------------*/

-- Step 1
-- We've created a database from a secure data share enabled by an external provider. 
-- This is read-only and query-ready.
-- source of trips, programs, stations data: snowflake_demo_resources.citibike_reset_v2.trips
select count(*) from snowflake_demo_resources.citibike_reset_v2.trips;
-- 97,513,269 records, also available on hover-on the table 
-- let's sample some data
select * from snowflake_demo_resources.citibike_reset_v2.trips limit 5;

-- list external S3 bucket content -> should show nothing
list @snowflake_demo.public.snowflake_demo_stage;

-- pre-req step - DONE
-- Step 3 - the S3 bucket has been setup for SQS event notification. Every time when a file lands in the S3 bucket a notification event is submitted to an SQS queue.
-- see this: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3.html

-- pre-req step - DO
-- Step 4 - Snowpipe processes these events as they come using a “Trips" pipe construct which provides resources to process the file. 
-- Data will be copied to a staging table, TRIPS_RAW.
create or replace pipe snowflake_demo.raw.trips_pipe auto_ingest=true as copy into snowflake_demo.raw.trips_raw from @snowflake_demo.public.snowflake_demo_stage/;

-- show pipes in current database/schema 
use schema snowflake_demo.raw; 
show pipes;

-- check pipe status
select system$pipe_status('trips_pipe');
-- as expected, there are no files pending

-- pre-req step - DO
-- Step 5 - Create streams to capture changes in the raw table
create or replace stream snowflake_demo.raw.new_trips on table snowflake_demo.raw.trips_raw;
create or replace stream snowflake_demo.raw.new_stations on table snowflake_demo.raw.trips_raw;
create or replace stream snowflake_demo.raw.new_programs on table snowflake_demo.raw.trips_raw;
use schema snowflake_demo.raw;
show streams;

-- utility procedures
use schema snowflake_demo.raw;
show procedures;

-- data producer stream (used at step 2)
describe procedure snowflake_demo.raw.stream_data (string, string);
-- source of trips, programs, stations data: snowflake_demo_resources.citibike_reset_v2.trips (little arrow on the left indicates a share)

-- target tables for step 4
-- RAW schema
use schema snowflake_demo.raw; 
show tables;
describe table snowflake_demo.raw.trips_raw; 
-- v VARIANT - stores semi-structured data, e.g. Parquet, JSON, ORC, XML, Avro.

-- step 6 TASKS
use schema snowflake_demo.modelled; 
show tasks; -- 3 push tasks on 1-minute schedule
describe task snowflake_demo.modelled.push_trips;    -- INSERT (always new transactional data)
describe task snowflake_demo.modelled.push_programs; -- MERGE (reference data updates)
describe task snowflake_demo.modelled.push_stations; -- MERGE (reference data updates)

-- target tables for step 6
-- MODELLED schema
use schema snowflake_demo.modelled; 
show tables;
describe table snowflake_demo.modelled.trips;
describe table snowflake_demo.modelled.stations;
describe table snowflake_demo.modelled.programs;

-- START DEMO (we have all pipeline components in place - let it flow)
-- Step 6 - Scheduled tasks (every minute, in this case) will take the new trips, programs and stations data and move it to the target tables, trips, programs and stations. 
-- resume suspended tasks
use schema snowflake_demo.modelled;
alter task snowflake_demo.modelled.push_trips resume;
alter task snowflake_demo.modelled.push_programs resume;
alter task snowflake_demo.modelled.push_stations resume;

-- Step 2
-- stream_data() stored procedure is used to simulate a real time data provider; in this case, this stored procedure 
-- will query the data presented at step 1 and generate a continuous inflow of files, JSON and compressed automatically. 
-- These files will be written to an S3 bucket. 
-- check whether we have some files from the previous run
list @snowflake_demo.public.snowflake_demo_stage;
-- Load some data using stream_data stored procedure

-- stream_data SP uses datapipeline_wh created as Small
-- let's increase the size to Large to speed-up the process
use warehouse datapipeline_wh;
alter warehouse datapipeline_wh set warehouse_size=large;
-- note the very short time need to scale-up

call snowflake_demo.raw.stream_data ('01/10/2020', '01/10/2020');
-- show files posted to the external stage (S3 bucket)

list @snowflake_demo.public.snowflake_demo_stage;
-- alternately
select distinct 's3://snowflake.demo/' || metadata$filename filename from @snowflake_demo.public.snowflake_demo_stage;

select $1 from @snowflake_demo.public.snowflake_demo_stage;
-- sample some data from the stage -> 51,767 rows
-- note querying directly S3 with SQL even without external tables, JSON

-- let's look at the data pipeline
select
  (select min(timestampdiff(second, current_timestamp, scheduled_time))
    from table(information_schema.task_history())
    where state = 'SCHEDULED' order by completed_time desc) time_to_next_pulse,
  (select count(distinct metadata$filename) from @snowflake_demo.public.snowflake_demo_stage/) files_in_bucket,
  (select parse_json(system$pipe_status('snowflake_demo.raw.trips_pipe')):pendingFileCount::number) pending_file_count,
  (select count(*)
    from table(information_schema.copy_history(
    table_name=>'snowflake_demo.raw.trips_raw',
    start_time=>dateadd(minute, -15, current_timestamp)))) files_processed,
  (select count(*) from snowflake_demo.raw.trips_raw) trips_raw,
  (select count(*) from snowflake_demo.raw.new_trips) recs_in_stream,
  (select count(*) from snowflake_demo.modelled.trips) trips_modelled,
  (select count(*) from snowflake_demo.modelled.programs) num_programs,
  (select count(*) from snowflake_demo.modelled.stations) num_stations,
  (select max(starttime) from snowflake_demo.modelled.trips) max_date;
  
-- now that we do not need such a Large warehouse, let's change it back to Small
alter warehouse datapipeline_wh set warehouse_size=small;
-- note the very short time need to scale-down

-- Step 5 -  Three table STREAMS, new_trips, new_programs, new_stations capture the new data. 
-- Snowpipe copies the data into our raw table
select count(*) from snowflake_demo.raw.trips_raw;
select * from snowflake_demo.raw.trips_raw limit 10; -- sample the data as JSON
-- Snowpipe should load 51,167 records into snowflake_demo.raw_trips_raw table;

-- and the insertions are tracked in the stream
-- count is zero if we did not move fast enough before they were processed
select count(*) from snowflake_demo.raw.new_trips;
select count(*) from snowflake_demo.raw.new_programs;
select count(*) from snowflake_demo.raw.new_stations;
-- check the target tables
select count(*) from snowflake_demo.modelled.trips; -- 51767
select count(*) from snowflake_demo.modelled.programs; -- 61
select count(*) from snowflake_demo.modelled.stations; -- 930

-- operational questions
-- task run history in the last 15 minutes
select * from table(information_schema.task_history())
  where scheduled_time > dateadd(minute, -15, current_time())
  and state <> 'SCHEDULED'
  order by completed_time desc;
-- note SUCEEDED and SKIPPED

-- how long to next task run? seconds
select timestampdiff(second, current_timestamp, scheduled_time) next_run, scheduled_time, name, state
  from table(information_schema.task_history())
  where state = 'SCHEDULED' order by completed_time desc;

-- how many files have been processed by the pipe in the last 15 minutes?
select count (*)
from table(information_schema.copy_history(
  table_name=>'snowflake_demo.raw.trips_raw',
  start_time=>dateadd(minute, -15, current_timestamp)));

-- this is the list of last 16 files that have been loaded by Snowpipe in the last 15 minutes
select top 16 *
from table(information_schema.copy_history(
  table_name=>'snowflake_demo.raw.trips_raw',
  start_time=>dateadd(minute, -15, current_timestamp)))
order by last_load_time desc;

-- for demo-sake, suspend tasks to save credits
alter task snowflake_demo.modelled.push_trips suspend;
alter task snowflake_demo.modelled.push_programs suspend;
alter task snowflake_demo.modelled.push_stations suspend;

-- check one of the tables we've built
select count(*) from snowflake_demo.modelled.trips;      -- 51,767
select count(*) from snowflake_demo.modelled.stations;   -- 930
select count(*) from snowflake_demo.modelled.programs;   -- 61

-- sample some data from one of the tables
select * from snowflake_demo.modelled.programs;
select * from snowflake_demo.modelled.stations;
select * from snowflake_demo.modelled.trips;

-- Step 7 - Relevant data is securely shared to external consumers via Reader Accounts.
-- e.g. NYCHA, JCHA - housing authorities that offer discount programs to their members
-- pre-req DONE
create or replace share snowflake_demo comment='Share trip data with housing authorities.';
-- go to Shares and show the Outbound share with no consumers
show shares like 'snowflake_demo%';

--what are we sharing?
-- pre-req DONE
grant usage on database snowflake_demo to share snowflake_demo;
grant usage on schema snowflake_demo.modelled to share snowflake_demo;

-- create a secure/restricted view that limits the amount of data available based on the
-- account of the viewer. It also selectively OBFUSCATES two of the columns (see MD5).
-- inner join security (filter by reader account)
-- current(account() UDF
-- pre-req - DONE - will run queries against it later
create or replace secure view trips_secure_vw as
  select current_account() as acct,
         p.program_name,
         date_trunc(hour, t.starttime) starttime_hr,
         iff(current_account() in (select account from snowflake_demo.modelled.security where name = 'Publisher Account'),
             sts.station_name, 'REDACTED (' || md5(sts.station_name) || ')') start_station_name,
         iff(current_account() in (select account from snowflake_demo.modelled.security where name = 'Publisher Account'),
             ste.station_name, 'REDACTED (' || md5(ste.station_name) || ')') end_station_name
  from snowflake_demo.modelled.trips t 
  inner join snowflake_demo.modelled.security s
  inner join snowflake_demo.modelled.programs p
  inner join snowflake_demo.modelled.stations sts
  inner join snowflake_demo.modelled.stations ste 
  where p.program_id = t.program_id
    and p.program_name like s.filter
    and sts.station_id = t.start_station_id
    and ste.station_id = t.end_station_id
    and s.account = current_account();

-- pre-req DONE
grant select on view snowflake_demo.modelled.trips_secure_vw to share snowflake_demo;
-- check the share
desc share snowflake_demo;
-- as you can see we granted access to database, schema and one one view

-- who are we sharing with?
-- we have created a security table with accounts and filter rules
-- pre-req - DONE
create or replace table snowflake_demo.modelled.security as select * from citibike.public.security;

select * from snowflake_demo.modelled.security;

-- grant access to NYCHA and JCHA accounts
set nycha = (select account from security where name = 'NYCHA');
set jcha = (select account from security where name = 'JCHA');

-- share with reader accounts
alter share snowflake_demo add accounts = $nycha, $jcha;

-- test that the security works
-- let's test what consumers will see - NYCHA
alter session set simulated_data_sharing_consumer = $nycha;
-- look at the numbers we can compare them in the reader account later
select count(*) from trips_secure_vw;
-- 2883
select program_name, acct, count(*) as "Num Trips"
  from snowflake_demo.modelled.trips_secure_vw
  group by 1,2
  order by 3 desc;

-- let's test what consumers will see - NYCHA
alter session set simulated_data_sharing_consumer = $jcha;
select count(*) from trips_secure_vw;
-- 641
select program_name, acct, count(*) as "Num Trips"
  from snowflake_demo.modelled.trips_secure_vw
  group by 1,2
  order by 3 desc;
  
-- unset
alter session unset simulated_data_sharing_consumer;

-- The data we are sharing is now locked down per consumer
-- go to NYCHA & JCHA accounts and query the data (reader accounts)
 
-- GOVERN ACCESS
revoke select on view snowflake_demo.modelled.trips_secure_vw from share snowflake_demo;
-- go to NYCHA and JCHA and show access is lost
grant select on view snowflake_demo.modelled.trips_secure_vw to share snowflake_demo;
-- go to NYCHA and JCHA and show access is regained


-- Step 8 - If all the data has been ingested successfully, the last task is to run the purge stored procedure to remove all the files from the external stage or just for the demo-sake run the following.
rm @snowflake_demo.public.snowflake_demo_stage;