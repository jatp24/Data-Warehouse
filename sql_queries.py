import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR NULL,
    auth VARCHAR NULL,
    firstName VARCHAR NULL,
    gender VARCHAR NULL,
    itemInSession VARCHAR NULL,
    lastName VARCHAR NULL,
    length VARCHAR NULL,
    level VARCHAR NULL,
    location VARCHAR NULL,
    method VARCHAR NULL,
    page VARCHAR NULL,
    registration VARCHAR NULL,
    sessionId INTEGER NULL SORTKEY DISTKEY,
    song VARCHAR NULL,
    status VARCHAR NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR NULL,
    userId INTEGER NULL
    
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER NULL,
    artist_id VARCHAR NOT NULL SORTKEY DISTKEY,
    artist_latitude VARCHAR NULL,
    artist_longitude VARCHAR NULL,
    artist_location VARCHAR NULL,
    artist_name VARCHAR NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NULL,
    duration DECIMAL NULL,
    year INTEGER NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL DISTKEY,
    level VARCHAR NOT NULL,
    song_id VARCHAR NULL,
    artist_id VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    location VARCHAR NULL,
    user_agent VARCHAR NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER NOT NULL SORTKEY,
    first_name VARCHAR NULL,
    last_name VARCHAR NULL,
    gender VARCHAR NULL,
    level VARCHAR NULL
    
    ) diststyle all;

""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR NOT NULL SORTKEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    duration DECIMAL NOT NULL
    );
""")

artist_table_create = ("""

    CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR NOT NULL SORTKEY,
    name VARCHAR NULL,
    location VARCHAR NULL,
    latitude DECIMAL NULL,
    longitude DECIMAL NULL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour SMALLINT NULL,
    day SMALLINT NULL,
    week SMALLINT NULL,
    month SMALLINT NULL,
    year SMALLINT NULL,
    weekday SMALLINT NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    json 'auto'
    STATUPDATE ON;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id,
                            artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
    e.userId as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId as session_id,
    e.location,
    e.userAgent as user_agent
    FROM staging_events e 
    LEFT JOIN staging_songs s 
    ON (e.song = s.title)
    AND (e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
    AND song_id IS NOT NULL;
""")

user_table_insert = ("""
    INSERT into users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
        FROM staging_events
        WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration as duration
        FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
    FROM staging_songs;
   
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year,
    EXTRACT(weekday from start_time) AS weekday
    FROM staging_events 
    WHERE page = 'NextSong';
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
