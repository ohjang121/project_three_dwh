import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# Add CASCADE to drop regardless of dependency
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist TEXT,
                                           auth TEXT,
                                           first_name TEXT,
                                           gender TEXT,
                                           item_in_session INT,
                                           last_name TEXT,
                                           length DOUBLE PRECISION,
                                           level TEXT,
                                           location TEXT,
                                           method TEXT,
                                           page TEXT,
                                           registration DOUBLE PRECISION,
                                           session_id INT,
                                           song TEXT,
                                           status INT,
                                           ts BIGINT,
                                           user_agent TEXT,
                                           user_id INT
                                           )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (artist_id TEXT,
                                          artist_latitude DOUBLE PRECISION,
                                          artist_location TEXT,
                                          artist_longitude DOUBLE PRECISION,
                                          artist_name TEXT,
                                          duration DOUBLE PRECISION,
                                          num_songs DOUBLE PRECISION,
                                          song_id TEXT,
                                          title TEXT,
                                          year INT
                                          )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, -- log_data.userId 
                                  first_name VARCHAR, -- log_data.firstName
                                  last_name VARCHAR, -- log_data.lastName
                                  gender VARCHAR, -- log_data.gender
                                  level VARCHAR -- log_data.level
                                  )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, -- song_data.song_id
                                  title VARCHAR NOT NULL,
                                  artist_id VARCHAR, -- song_data.artist_id
                                  year INT, -- song_data.year
                                  duration DOUBLE PRECISION NOT NULL -- song_data.duration
                                  )
SORTKEY (year)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, -- song_data.artist_id
                                    name VARCHAR NOT NULL, -- song_data.artist_name
                                    location VARCHAR, -- song_data.artist_location
                                    latitude DOUBLE PRECISION, -- song_data.artist_latitude
                                    longitude DOUBLE PRECISION -- song_data.artist_longitude
                                    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, -- log_data.ts
                                 hour INT, 
                                 day INT,
                                 week INT,
                                 month INT,
                                 year INT,
                                 weekday INT
                                 );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY, -- autoincrement surrogate key, not in the log file
                                      start_time TIMESTAMP NOT NULL REFERENCES time(start_time), -- log_data.ts
                                      user_id INT NOT NULL REFERENCES users(user_id), -- log_data.userId
                                      level VARCHAR, -- log_data.level
                                      song_id VARCHAR REFERENCES songs(song_id), -- song_data.song_id
                                      artist_id VARCHAR REFERENCES artists(artist_id), -- song_data.artist_id
                                      session_id INT, -- log_data.sessionId
                                      location VARCHAR, -- log_data.location
                                      user_agent VARCHAR -- log_data.userAgent
                                      )
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events (
    artist,
    auth,
    first_name,
    gender,
    item_in_session,
    last_name,
    length,
    level,
    location,
    method,
    page,
    registration,
    session_id,
    song,
    status,
    ts,
    user_agent,
    user_id
)
from {config['S3']['LOG_DATA']}
iam_role {config['IAM_ROLE']['IAM_ROLE_ARN']}
FORMAT JSON AS {config['S3']['LOG_JSONPATH']}
""")

staging_songs_copy = (f"""
COPY staging_songs (
    artist_id,
    artist_latitude,
    artist_location,
    artist_longitude,
    artist_name,
    duration,
    num_songs,
    song_id,
    title,
    year
)
from {config['S3']['SONG_DATA']} 
iam_role {config['IAM_ROLE']['IAM_ROLE_ARN']}
FORMAT JSON AS 'auto'
TRUNCATECOLUMNS
""")

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
first_name,
last_name,
gender,
level
FROM staging_events
WHERE user_id is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
WHERE artist_id is not null
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
extract(hour from start_time) as hour,
extract(day from start_time) as day,
extract(week from start_time) as week,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(weekday from start_time) as weekday
FROM staging_events
WHERE ts is not null
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
e.user_id,
e.level,
s.song_id,
a.artist_id,
e.session_id,
e.location,
e.user_agent
FROM staging_events e
JOIN songs s on e.song = s.title
JOIN artists a on e.artist = a.name
WHERE e.page = 'NextSong'
AND e.user_id is not null
AND s.song_id is not null
AND a.artist_id is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
