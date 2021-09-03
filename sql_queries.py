import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
HOST = config.get("CLUSTER","HOST")
DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(artist varchar,
auth text,
firstName varchar,
gender text,
itemInSession int,
lastName varchar,
length decimal,
level text,
location varchar,
method text,
page text,
registration decimal,
sessionId int,
song varchar,
status int,
ts bigint,
userAgent varchar,
userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs   
(artist_id varchar, 
artist_latitude decimal,
artist_location varchar,
artist_longitude decimal,
artist_name varchar,
duration decimal,
num_songs int,
song_id varchar,
title varchar,
year int)
""")

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS users 
(user_id int NOT NULL sortkey, 
first_name text, 
last_name text, 
gender text, 
level text);
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs 
(song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar NOT NULL, 
year int, 
duration decimal);
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artists 
(artist_id varchar PRIMARY KEY, 
name text, 
location varchar, 
latitude decimal, 
longitude decimal);
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time 
(start_time timestamp PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int);
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id int IDENTITY(0,1), 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level text, 
song_id varchar, 
artist_id varchar, 
session_id int, 
location varchar, 
user_agent varchar);
""")

# STAGING TABLES

staging_events_copy = (""" 
COPY staging_events 
FROM {}
credentials 'aws_iam_role={}'
compupdate off
region 'us-west-2'
FORMAT AS json {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
credentials 'aws_iam_role={}'
compupdate off
region 'us-west-2'
FORMAT AS json 'auto';
""").format(SONG_DATA, DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplays 
(start_time , user_id , level , song_id , artist_id , session_id , location , user_agent) 
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, 
se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, ss.artist_location, se.userAgent
FROM staging_events se
LEFT JOIN staging_songs ss
ON (ss.title = se.song AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
AND se.userId IS NOT NULL;
""")

user_table_insert = (""" 
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong'
AND userId NOT IN (SELECT DISTINCT user_id FROM users)
AND userId IS NOT NULL;
""")

song_table_insert = (""" 
INSERT INTO songs 
(song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
AND song_id IS NOT NULL;
""")

artist_table_insert = (""" 
INSERT INTO artists 
(artist_id, name, location, latitude, longitude) 
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songS
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
AND artist_id IS NOT NULL;
""")


time_table_insert = (""" 
INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, 
extract(hour from start_time) as hour,
extract(day from start_time) as day,
extract(week from start_time) as week,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(dow from start_time) as weekday
FROM staging_events se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, time_table_drop, user_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
