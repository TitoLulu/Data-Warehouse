import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table
    (
        artist varchar(400),
        auth varchar(100),
        firstname varchar(100),
        gender varchar(1),
        iteminsession int,
        lastname varchar(100),
        length float,
        level varchar(100),
        location varchar(100),
        method varchar(100),
        page varchar(100),
        registration float,
        sessionid int,
        song varchar(400),
        status int,
        ts varchar(200),
        useragent varchar(200),
        userid int
        
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table
    (
        artist_id varchar(100),
        artist_latitude float,
        artist_location varchar(400),
        artist_longitude float,
        artist_name varchar(400),
        duration float,
        num_songs int,
        song_id varchar(100),
        title varchar(400),
        year  int
    )
    ;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
    (
        songplay_id int identity (1,1) primary key, 
        start_time timestamp not null, 
        user_id int not null, 
        level varchar(100), 
        song_id varchar(100), 
        artist_id varchar(100), 
        session_id varchar(100), 
        location varchar(400), 
        user_agent varchar(400)
    )
    SORTKEY(songplay_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table
    (
        user_id varchar(100) primary key, 
        first_name varchar(100), 
        last_name varchar(100), 
        gender varchar(1), 
        level varchar(100)
    )
    SORTKEY(user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
    (
        song_id varchar(100) primary key, 
        title varchar(400), 
        artist_id varchar(100), 
        year int, 
        duration varchar(100)
    )
    SORTKEY(song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id varchar(100) primary key, 
        name varchar(400), 
        location varchar(400), 
        latitude float, 
        longitude float
    )
    SORTKEY(artist_id)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table
    (
        start_time timestamp primary key, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    )SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events_table 
    from '{}'
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    JSON '{}'
    COMPUPDATE OFF STATUPDATE OFF
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs_table
    from '{}'
    credentials 'aws_iam_role={}' 
    region 'us-west-2' 
    JSON 'auto'
    COMPUPDATE OFF STATUPDATE OFF
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
 INSERT INTO songplay_table 
        (
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
    SELECT 
        distinct timestamp 'epoch' + cAST(e.ts as bigint)/1000 * interval '1 second' as start_time,
        e.userid as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid as session_id,
        e.location,
        e.useragent as user_agent
    FROM staging_events_table e 
    JOIN staging_songs_table s on (e.artist = s.artist_name and e.song = s.title e.length = s.duration)
    WHERE e.page ilike '%%nextsong%%'
    and s.song_id is not null;
""")

user_table_insert = ("""
    INSERT INTO user_table 
    (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    SELECT 
        distinct userid as user_id,
        firstname as first_name,
        lastname as last_name,
        gender,
        level 
    FROM staging_events_table e
    WHERE e.userid is not null
""")

song_table_insert = ("""
    INSERT INTO song_table
    (
        song_id, 
        title, 
        artist_id, 
        year,
        duration
    ) 
    SELECT 
        distinct song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs_table s
    WHERE song_id is not null;
""")

artist_table_insert = ("""
    INSERT INTO artist_table
    (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    ) 
   SELECT
       distinct artist_id,
       artist_name as name,
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
    FROM staging_songs_table s
    WHERE artist_id is not null
""")

time_table_insert = ("""
   INSERT INTO time_table 
    (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT 
        distinct timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second' as start_time,
        EXTRACT(hour from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as hour,
        EXTRACT(day from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as day,
        EXTRACT(week from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as week,
        EXTRACT(month from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as month,
        EXTRACT(year from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as year,
        EXTRACT(weekday from timestamp 'epoch' + CAST(ts as bigint)/1000 * interval '1 second') as weekday
    FROM staging_events_table
    WHERE ts is not null;
""")

# QUERY LISTS

create_table_queries = [staging_events_table,staging_songs_table,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table,staging_songs_table,songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
