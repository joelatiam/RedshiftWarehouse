import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
event_id BIGINT IDENTITY(1,1) PRIMARY KEY,
artist_name VARCHAR(255),
auth VARCHAR(50),
user_first_name VARCHAR(255),
user_gender VARCHAR(1),
item_in_session INTEGER,
user_last_name VARCHAR(255),
song_length DOUBLE PRECISION, 
user_level VARCHAR(50),
location VARCHAR(255),
method VARCHAR(4),
page VARCHAR(35),
registration VARCHAR(50),
session_id BIGINT,
song_title VARCHAR(255),
status SMALLINT, 
ts BIGINT,
user_agent TEXT,
user_id BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
num_songs INT,
artist_id VARCHAR,
artist_latitude DECIMAL,
artist_longitude DECIMAL,
artist_location VARCHAR,
artist_name VARCHAR(255),
song_id VARCHAR,
title VARCHAR(255),
duration DOUBLE PRECISION,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
start_time timestamp sortkey,
user_id BIGINT distkey,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INT,
location VARCHAR,
user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id BIGINT PRIMARY KEY sortkey distkey,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR,
level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id VARCHAR PRIMARY KEY,
title VARCHAR(255) NOT NULL,
artist_id VARCHAR NOT NULL distkey,
year INT sortkey,
duration DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR PRIMARY KEY sortkey,
name VARCHAR NOT NULL,
location VARCHAR,
latitude DECIMAL,
longitude DECIMAL
)
diststyle all
;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday INT NOT NULL
)
diststyle all
;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
iam_role {}
format as json {}
region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
json 'auto'
region 'us-west-2';
""").format(SONG_DATA, IAM_ROLE)

# FILTERS TO RETRIEVE LATEST AND UNIQUE STAGING RECORDS
distinct_events_id_with_users_id = ("""
SELECT latest_event_id
FROM (
	SELECT user_id, max(event_id) AS latest_event_id
    FROM staging_events
    WHERE user_id IS NOT NULL
	group by user_id
	) filter_records
""")

distinct_songs_id = ("""
SELECT latest_artist_data_id
FROM (
	SELECT song_id, max(songplay_id) AS latest_artist_data_id
    FROM staging_songs
    WHERE song_id IS NOT NULL
	group by song_id
	) filter_records
""")

distinct_artists_id = ("""
SELECT latest_artist_data_id
FROM (
	SELECT artist_id, max(songplay_id) AS latest_artist_data_id
    FROM staging_songs
    WHERE artist_id IS NOT NULL
	group by artist_id
	) filter_records
""")

distinct_times = ("""
SELECT
DISTINCT(
	TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second'
) AS ts
FROM staging_events
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
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
(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second') AS ts,
e.user_id, e.user_level, s.song_id,
s.artist_id, e.session_id, e.location,
e.user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON e.song_title = s.title
""")

user_table_insert = ("""
INSERT INTO users (
user_id, first_name,
last_name, gender,
level
)
SELECT user_id,
user_first_name, user_last_name,
user_gender, user_level
FROM staging_events
WHERE event_id IN ({});
""").format(distinct_events_id_with_users_id)

song_table_insert = ("""
INSERT INTO songs(
song_id,
title,
artist_id,
year,
duration
)
SELECT
song_id, title, artist_id, year, duration
FROM staging_songs
WHERE songplay_id IN ({});
""").format(distinct_songs_id)

artist_table_insert = ("""
INSERT INTO artists (
artist_id,
name,
location,
latitude,
longitude
)
SELECT
artist_id, artist_name, artist_location,
artist_latitude, artist_longitude
FROM staging_songs
WHERE songplay_id IN ({});
""").format(distinct_artists_id)

time_table_insert = ("""
INSERT INTO time (
start_time,
hour,
day,
week,
month,
year,
weekday
)
SELECT ts,
EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts),
EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts),
EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
FROM({});
""").format(distinct_times)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create,
    song_table_create, artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop,
    songplay_table_drop, user_table_drop, song_table_drop,
    artist_table_drop, time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert, user_table_insert,
    song_table_insert, artist_table_insert,
    time_table_insert
]
