## Project: Datawarehouse

ETL Pipeline for fictional company, Sparkify, that uses datasets provided to create a star schema optimized for queries on song play analysis.

## Datasets 

song dataset is a subset of real data from [](http://millionsongdataset.com/):**Million Song Dataset**

log dataset is generated by this [](https://github.com/Interana/eventsim):**Event Simulator** 

The two datasets reside inside s3

   1. Song data: s3://udacity-dend/song_data
    
   2. Log data: s3://udacity-dend/log_data
    
The path to JSON log dataset is s3://udacity-dend/log_json_path.json

## Approach 

Data is loaded from s3 into redshift staging tables (staging_events_table, staging_songs_table).

Execution of SQL statements to create analytics tables and insert data into the resulting analytics tables.

Resulting tables are as follows:

 1. songplays(fact table) - records in event data associated with song plays i.e. records with page NextSong
    |songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent|
    |-----------|----------|-------|-----|-------|---------|----------|--------|----------|
    
 2. users - app users
     
     |first_name|last_name|gender|level|
     |:---------|:--------|:-----|:----|
     
 3. songs - songs in app database
     
     |song_id|title|artist_id|year|duration|
     |:------|:----|:--------|:---|:-------|
     
 4. artists - artists in app database
 
     |artist_id|name|location|latitude|longitude|
     |:--------|:---|:-------|:-------|:--------|
     
 5. time  - timestamps of records on songplays broken down into specific units
 
     |start_time|hour|day|week|month|year|weekday|
     |:---------|:---|:--|:---|:----|:---|:------|
     
## Reference 

-[Reference #1](https://docs.aws.amazon.com/redshift/latest/mgmt/python-basic-test-example.html):**Performing a basic connector test with NumPy integration**

-[Reference #2](https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift):**How to convert epoch to datetime redshift**

-[Reference #3](https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html):**Using the COPY command to load from Amazon S3**
 
  

