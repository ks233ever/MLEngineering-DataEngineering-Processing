import configparser
from keys import params

S3 = params['S3']


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage;"
staging_songs_table_drop =  "DROP TABLE IF EXISTS songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE events_stage(
        artist              TEXT,
        auth                TEXT,
        firstName           TEXT,
        gender              TEXT,
        itemInSession       INTEGER,
        lastName            TEXT,
        length              NUMERIC,
        level               TEXT,
        location            TEXT,
        method              TEXT,
        page                TEXT,
        registration        NUMERIC,
        sessionId           INTEGER,
        song                TEXT,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           TEXT,
        userId              INTEGER 
    )
""")


staging_songs_table_create = ("""
    CREATE TABLE songs_stage(
        num_songs           INTEGER,
        artist_id           TEXT,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            NUMERIC,
        year                INTEGER
    )
""")


songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
        user_id INTEGER NOT NULL,
        level TEXT,
        song_id TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT
    )
""")


user_table_create = user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER PRIMARY KEY SORTKEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT     
    )
""")


song_table_create = ("""
    CREATE TABLE songs(
        song_id TEXT PRIMARY KEY SORTKEY,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INTEGER,
        duration FLOAT
    )
""")



artist_table_create = ("""
    CREATE TABLE artists(
        artist_id TEXT PRIMARY KEY SORTKEY,
        name TEXT NOT NULL,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
""")


time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP DISTKEY SORTKEY PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER     
    )
""")






# STAGING TABLES

staging_events_copy = ("""
    copy events_stage FROM {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=S3['log_data'], role_arn=S3['iam_role'], log_json_path=S3['log_jsonpath'])



staging_songs_copy = ("""
    copy songs_stage from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=S3['log_data'], role_arn=S3['iam_role'])




# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  as start_time, 
            e.userId as user_id, 
            e.level as level, 
            s.song_id as song_id, 
            s.artist_id as artist_id, 
            e.sessionId as session_id, 
            e.location as location, 
            e.userAgent as user_agent
    FROM events_stage e
    JOIN songs_stage s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
    AND e.page = 'NextSong'
 
""")




user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId) as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
    FROM events_stage
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")



song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) as song_id,
            title,
            artist_id,
            year,
            duration
    FROM songs_stage
    WHERE song_id IS NOT NULL;
""")




artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) as artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
    FROM songs_stage
    WHERE artist_id IS NOT NULL;
""")




time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time) as start_time,
            EXTRACT(hour FROM start_time) as hour,
            EXTRACT(day FROM start_time) as day,
            EXTRACT(week FROM start_time) as week,
            EXTRACT(month FROM start_time) as month,
            EXTRACT(year FROM start_time) as year,
            EXTRACT(dayofweek FROM start_time) as weekday
    FROM songplays;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
