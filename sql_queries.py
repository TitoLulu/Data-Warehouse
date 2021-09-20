import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP IF EXISTS user_table"
song_table_drop = "DROP IF EXISTS song_table"
artist_table_drop = "DROP IF EXISTS artist_table"
time_table_drop = "DROP IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events
    (
        artist varchar,
        auth varchar,
        firstname varchar,
        gender varchar,
        iteminsession int,
        lastname varchar,
        length float,
        level varchar,
        method varchar,
        page varchar,
        registration float,
        sessionid int,
        song varchar,
        status int,
        ts numeric,
        useragent varchar,
        userid int
        
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year  int
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
    (
        songplay_id identity(1,0) primary key, 
        start_time timestamp not null, 
        user_id int not null, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id varchar, 
        location varchar, 
        user_agent varchar
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table
    (
        user_id int primary key, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
    (
        song_id varchar primary key, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration varchar
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
    (
        artist_id varchar primary key, 
        name varchar, 
        location varchar, 
        lattitude float, 
        longitude float
    );
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
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY events 
    [
        artist,
        auth,
        firstname,
        gender,
        iteminsession,
        lastname,
        length,
        level,
        method,
        page,
        registration,
        sessionid,
        song,
        status,
        ts,
        useragent,
        userid 
    ]
    from 's3://udacity-dend/log_data'
    credentials 'aws_access_key_id={};aws_secret_access_key={}' 
options;
""").format(*config['CLUSTER'].values())

staging_songs_copy = ("""
COPY songs
    [
        num_songs ,
        artist_idr,
        artist_latitude,
        artist_longitude,
        artist_location,
        artist_name,
        song_id,
        title,
        duration,
        year
    ]
    from 's3://udacity-dend/song_data'
    credentials 'aws_access_key_id={};aws_secret_access_key={}' 
options;
""").format(*config['CLUSTER'].values())

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO TABLE songplay_table 
        (
            songplay_id,
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
    VALUES (DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s)
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
    VALUES(%s,%s,%s,%s,%s)
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
    VALUES(%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""
    INSET INTO artist_table
    (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    ) 
    VALUES(%s,%s,%s,%s,%s)
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
    VALUES(%s,%s,%s,%s,%s,%s,%s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
