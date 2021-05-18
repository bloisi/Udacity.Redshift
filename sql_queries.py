import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
rep_tab_users_drop = "DROP TABLE IF EXISTS rep_users"
rep_tab_artists_drop = "DROP TABLE IF EXISTS rep_artists"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR, auth  VARCHAR, \
firstName VARCHAR, gender VARCHAR, itemInSession INT, lastName VARCHAR, length NUMERIC, level VARCHAR, \
location VARCHAR, method VARCHAR,  page VARCHAR, registration REAL, sessionId INT, song VARCHAR, \
status INT, ts TIMESTAMP, useragent VARCHAR, userId INT);""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT, artist_id VARCHAR, \
artist_latitude REAL, artist_longitude REAL, artist_location VARCHAR, artist_name VARCHAR, song_id VARCHAR, \
title VARCHAR, duration NUMERIC, year INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint IDENTITY(0,1), \
start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, level VARCHAR, song_id VARCHAR distkey NOT NULL, artist_id VARCHAR NOT NULL, \
session_id INT, location VARCHAR, user_agent VARCHAR, PRIMARY KEY (songplay_id));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INT sortkey NOT NULL, first_name VARCHAR, last_name VARCHAR, \
gender VARCHAR, level VARCHAR);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR sortkey distkey, title VARCHAR, artist_id VARCHAR, \
year INT, duration NUMERIC, PRIMARY KEY (song_id));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR sortkey NOT NULL, name VARCHAR, location VARCHAR, \
latitude NUMERIC, longitude NUMERIC);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP sortkey, hour INT, day INT, week INT, \
month INT, year INT, weekday INT, PRIMARY KEY (start_time));""")

rep_table_users = ("""CREATE TABLE IF NOT EXISTS rep_users (user_id INT sortkey, first_name VARCHAR, last_name VARCHAR, \
gender VARCHAR, level VARCHAR, rw_id INT);""")

rep_table_artists = ("""CREATE TABLE IF NOT EXISTS rep_artists (artist_id VARCHAR sortkey, name VARCHAR, location VARCHAR, \
latitude NUMERIC, longitude NUMERIC, rw_id INT);""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} \
    CREDENTIALS 'aws_iam_role={}' \
    COMPUPDATE OFF region 'us-west-2' \
    TIMEFORMAT as 'epochmillisecs' \
    FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2' 
    json 'auto'
""").format( config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
SELECT DISTINCT staging_events.ts, staging_events.userId, staging_events.level, stg_song.song_id, stg_artist.artist_id, staging_events.sessionId, \
staging_events.location, staging_events.useragent \
FROM staging_events staging_events \
LEFT JOIN staging_songs stg_song ON (staging_events.song = stg_song.title) \
LEFT JOIN staging_songs stg_artist ON (staging_events.artist = stg_artist.artist_name) \
where page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events where page = 'NextSong';""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
SELECT DISTINCT artist_id, artist_name,  artist_location, artist_latitude, artist_longitude FROM staging_songs;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
SELECT DISTINCT ts, extract(hour from ts) as hour, extract(day from ts) as day, \
extract(week from ts) as week, extract(month from ts) as month, extract(year from ts) as year, \
extract(weekday from ts) as weekday FROM staging_events where page = 'NextSong';""")

rep_tab_users_insert = ("""INSERT INTO rep_users (user_id, first_name, last_name, \
gender, level, rw_id) \
SELECT user_id, first_name, last_name, gender, level, \
row_number() OVER (partition BY user_id) rw_id \
FROM users \
WHERE user_id IN (SELECT user_id FROM users GROUP BY user_id HAVING count(*) > 1);""")

rep_tab_artists_insert = ("""INSERT INTO rep_artists (artist_id, name, location, \
latitude, longitude, rw_id) \
SELECT artist_id, name, location, latitude, longitude, \
row_number() OVER (partition BY artist_id ORDER BY name) rw_id \
FROM artists \
WHERE artist_id IN (SELECT artist_id FROM artists GROUP BY artist_id HAVING count(*) > 1);""")

# ELIMINATE DUPLICITY

rep_tab_users_clean = ("""DELETE FROM users WHERE user_id IN (SELECT user_id FROM rep_users);""")

rep_tab_artists_clean = ("""DELETE FROM artists WHERE artist_id IN (SELECT artist_id FROM rep_artists);""")

rep_tab_users_unique = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
SELECT rep_users1.user_id, rep_users1.first_name, rep_users1.last_name, rep_users1.gender, rep_users1.level \
FROM rep_users rep_users1, \
(select user_id, MAX(rw_id) rw_id from rep_users group by user_id) rep_users2 \
where rep_users1.user_id = rep_users2.user_id \
and   rep_users1.rw_id = rep_users2.rw_id;""") 

rep_tab_artists_unique = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
select rep_artists1.artist_id, rep_artists1.name, rep_artists1.location, rep_artists1.latitude, rep_artists1.longitude \
from rep_artists rep_artists1, \
(select artist_id, MAX(rw_id) rw_id from rep_artists group by artist_id) rep_artists2 \
where rep_artists1.artist_id = rep_artists2.artist_id \
and   rep_artists1.rw_id = rep_artists2.rw_id;""")

alter_users = ("""ALTER TABLE users ADD CONSTRAINT pkuser PRIMARY KEY(user_id);""")

alter_artists = ("""ALTER TABLE artists ADD CONSTRAINT pkartist PRIMARY KEY(artist_id);""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, rep_table_users, rep_table_artists]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, rep_tab_users_drop, rep_tab_artists_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert, rep_tab_users_insert, 
rep_tab_artists_insert]
eliminate_dup_queries = [rep_tab_users_clean, rep_tab_artists_clean, rep_tab_users_unique, rep_tab_artists_unique]
alter_table = [alter_users, alter_artists]