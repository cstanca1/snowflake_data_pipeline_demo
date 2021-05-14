-- STORAGE INTEGRATION AND STAGE
-- use option 1 described here: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html
create or replace storage integration YOURSRTORAGEINTEGRATION
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::YOURAWSACCOUNTNUMBER:role/YOURAWSROLE'
  storage_allowed_locations = ('YOURS3BUCKET/');
  
DESC INTEGRATION YOURSRTORAGEINTEGRATION;

create database it not exists snowflake_demo;
use database snowflake_demo;
use schema public;

grant create stage on schema public to role accountadmin;
grant usage on integration snowflake_demo_int to role accountadmin;

create or replace stage snowflake_demo_stage
  storage_integration = YOURSRTORAGEINTEGRATION
  url = 's3://snowflake.demo/';

use database snowflake_demo;
show stages;
list @snowflake_demo.public.snowflake_demo_stage;

-- DONE - DON'T DO IT AGAIN

/*--------------------------------------------------------------------------------
  In order to mimic periodically arriving data, this stored proc trickle-unloads data 
  from the TRIPS table in the RESET DB into JSON files in the stage. 
  The procedure takes a start and stop date range and aggregates records on a daily basis 
  and then writes the files out to the bucket location in 5 second intervals. 
  When it is run, it will produce a steady flow of data arriving in your defined STAGE object.
  --------------------------------------------------------------------------------*/

create or replace procedure snowflake_demo.raw.stream_data (START_DATE STRING, STOP_DATE STRING)
  returns float
  language javascript strict
as
$$
  var counter = 0;

  // list the partition values
  var days = snowflake.execute({ sqlText: "select distinct year(starttime) || '-' ||" +
                                                " iff(month(starttime)<10, '0' || month(starttime), '' || month(starttime)) || '-' ||" +
                                                " iff(day(starttime)<10, '0' || day(starttime), '' || day(starttime))" +
                                                " from snowflake_demo_resources.citibike_reset_v2.trips" +
                                                " where to_date(starttime) >= to_date('" + START_DATE + "')" +
                                                "   and to_date(starttime) <= to_date('" + STOP_DATE + "')" +
                                                " order by 1;" });

  // for each partition
  while (days.next())
  {
    var day = days.getColumnValue(1);
    var unload_qry = snowflake.execute({ sqlText: "copy into @snowflake_demo.public.snowflake_demo_stage/snowpipe_demo" + day + " from (" +
                                                  " select object_construct(" +
                                                  "   'tripduration', tripduration," +
                                                  "   'starttime', starttime," +
                                                  "   'stoptime', stoptime," +
                                                  "   'start_station_id', start_station_id," +
                                                  "   'start_station_name', ss.station_name," +
                                                  "   'start_station_latitude', ss.station_latitude," +
                                                  "   'start_station_longitude', ss.station_longitude," +
                                                  "   'end_station_id', end_station_id," +
                                                  "   'end_station_name', es.station_name," +
                                                  "   'end_station_latitude', es.station_latitude," +
                                                  "   'end_station_longitude', es.station_longitude," +
                                                  "   'bikeid', bikeid," +
                                                  "   'usertype', usertype," +
                                                  "   'birth_year', birth_year," +
                                                  "   'gender', gender," +
                                                  "   'program_id', t.program_id," +
                                                  "   'program_name', program_name)" +
                                                  " from snowflake_demo_resources.citibike_reset_v2.trips t " +
                                                  "              inner join snowflake_demo_resources.citibike_reset_v2.stations ss on t.start_station_id = ss.station_id" +
                                                  "              inner join snowflake_demo_resources.citibike_reset_v2.stations es on t.end_station_id = es.station_id" +
                                                  "              inner join snowflake_demo_resources.citibike_reset_v2.programs p on t.program_id = p.program_id" +
                                                  " where to_date(starttime) = to_date('" + day + "')" +
                                                  " order by starttime);" });

    counter++;

    // sleep for five seconds
    var wake = new Date();
    var now = new Date();
    wake = Date.now() + 1000;
    do { now = Date.now(); }
      while (now <= wake);
}
  return counter;
$$;

-- Stored Procedure for Cleaning Up Stage Files after Successful Load
-- In this lab, data being written to the STAGE by the STREAM_DATA stored procedure will be automatically loaded into a table via Snowpipe. 
-- The procedure here will be used to clean up files that are successfully loaded. 
-- It will compare records entries in the information_schema.copy_history view with the file names still present in the stage. 
-- Files that were found to have been loaded are now safely deleted by the procedure.

create or replace procedure snowflake_demo.raw.purge_files (TABLE_NAME STRING, STAGE_NAME STRING, BUCKET_URL STRING, BUCKET_SUBDIR STRING)
returns real
language javascript strict
execute as caller
as
$$
  var counter = 0;
  var files = snowflake.execute( {sqlText: `
  select 
    h.file_name
    from table(information_schema.copy_history( table_name=>'` + TABLE_NAME + `', start_time=>dateadd(hour, -10, current_timestamp))) h
    inner join (select distinct '` + BUCKET_URL + `' || metadata$filename filename from '` + STAGE_NAME + `') f
        on f.filename = (h.stage_location || h.file_name)
    where h.error_count = 0;`} );
  
  // for each file
  while (files.next())
  {
    var file = files.getColumnValue(1);
    sqlRemove = "rm " + STAGE_NAME + BUCKET_SUBDIR + file;
    try {
        var unload_qry = snowflake.execute({ sqlText: sqlRemove });
        counter++;
    }catch(err){
        //logging here
    }

}
  return counter;
$$;


-- allocate a warehouse for various checks during development of this pipeline
create warehouse if not exists datapipeline_wh with warehouse_size = 'small' auto_suspend = 60 initially_suspended = true;

-- database, schemas and tables are precreated for this demo, but this demo can also recreate all the objects from scratch
create schema if not exists snowflake_demo.raw;
create schema if not exists snowflake_demo.modelled;

create table if not exists snowflake_demo.raw.trips_raw (v variant); --  VARIANT data type - this will be used to store JSON data

/*--------------------------------------------------------------------------------
  Create a modelled schema into which we will move the data from the raw table.

  trips_raw  ->  trips      (convert to structured, append new records)
             ->  programs   (build a dimension table, merge new programs)
             ->  stations   (build a dimension table, merge new start and end stations)
--------------------------------------------------------------------------------*/

create table if not exists snowflake_demo.modelled.trips (
  tripduration integer,
  starttime timestamp_ntz,
  stoptime timestamp_ntz,
  start_station_id integer,
  end_station_id integer,
  bikeid integer,
  usertype string,
  birth_year integer,
  gender integer,
  program_id integer);


create table if not exists snowflake_demo.modelled.stations (
  station_id integer,
  station_name string,
  station_latitude float,
  station_longitude float,
  station_comment string
);


create table if not exists snowflake_demo.modelled.programs (
  program_id integer,
  program_name string
);


/*--------------------------------------------------------------------------------
  Tasks allow us to define and orchestrate the ELT logic. We consume the records
  collected by the streams by running DML transactions (insert, merge). This
  resets the stream so we can capture changes going forward from here.
--------------------------------------------------------------------------------*/

-- create a warehouse to run tasks
create warehouse if not exists task_wh with warehouse_size = 'small' auto_suspend = 60 initially_suspended = true;

-- push the trip data into modelled TRIPS
create or replace task snowflake_demo.modelled.push_trips warehouse = task_wh
  schedule = '1 minute'
  when system$stream_has_data('snowflake_demo.raw.new_trips')
as
insert into snowflake_demo.modelled.trips
  select v:tripduration::integer,
  v:starttime::timestamp_ntz,
  v:stoptime::timestamp_ntz,
  v:start_station_id::integer,
  v:end_station_id::integer,
  v:bikeid::integer,
  v:usertype::string,
  v:birth_year::integer,
  v:gender::integer,
  v:program_id::integer
  from snowflake_demo.raw.new_trips;


-- merge any new program records into modelled PROGRAMS
create or replace task snowflake_demo.modelled.push_programs warehouse = task_wh
  schedule = '1 minute'
  when system$stream_has_data('snowflake_demo.raw.new_programs')
as
merge into snowflake_demo.modelled.programs p
  using (
    select distinct v:program_id::integer program_id,
      v:program_name::string program_name
    from snowflake_demo.raw.new_programs) np
  on p.program_id = np.program_id
  when not matched then
    insert (program_id, program_name)
    values (np.program_id, np.program_name);


-- merge any new station records into modelled STATIONS
create or replace task snowflake_demo.modelled.push_stations warehouse = task_wh
  schedule = '1 minute'
  when system$stream_has_data('snowflake_demo.raw.new_stations')
as
merge into snowflake_demo.modelled.stations s
  using (
    select v:start_station_id::integer station_id,
      v:start_station_name::string station_name,
      v:start_station_latitude::float station_latitude,
      v:start_station_longitude::float station_longitude,
      'Station at ' || v:start_station_name::string station_comment
    from snowflake_demo.raw.new_stations
    union
    select v:end_station_id::integer station_id,
      v:end_station_name::string station_name,
      v:end_station_latitude::float station_latitude,
      v:end_station_longitude::float station_longitude,
      'Station at ' || v:end_station_name::string station_comment
    from snowflake_demo.raw.new_stations) ns
  on s.station_id = ns.station_id
  when not matched then
    insert (station_id, station_name, station_latitude, station_longitude, station_comment)
    values (ns.station_id, ns.station_name, ns.station_latitude, ns.station_longitude, ns.station_comment);


-- purge successfully loaded files from stage
create or replace task snowflake_demo.modelled.purge_files warehouse = task_wh
  after snowflake_demo.modelled.push_trips
as
  call snowflake_demo.raw.purge_files('trips_raw', '@SNOWFLAKE_DEMO.public.snowflake_demo_stage/', 'YOURS3BUCKET/', '');
  
  
