import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events (
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession integer,
lastName varchar,
length numeric,
level varchar,
location varchar ,
method varchar,
page varchar,
registration numeric,
sessionId integer,
song varchar,
status integer,
ts bigint,
userAgent varchar,
userId integer



)


""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
num_songs integer,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar ,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year integer

)


""")




songplay_table_create = ("""
create table if not exists songplays (
    songplay_id INTEGER IDENTITY (1, 1) primary key distkey,
    start_time timestamp sortkey,
    user_id int,
    level varchar,
    song_id varchar,
    artist_id varchar, 
    session_id int NOT NULL,
    location varchar,
    user_agent varchar,
     CONSTRAINT fk_user
       FOREIGN KEY(user_id) 
          REFERENCES users(user_id),
    CONSTRAINT fk_time
       FOREIGN KEY(start_time) 
          REFERENCES time(start_time),
    CONSTRAINT fk_song
       FOREIGN KEY(song_id) 
          REFERENCES songs(song_id),
    CONSTRAINT fk_artist
       FOREIGN KEY(artist_id) 
          REFERENCES artists(artist_id)
   
)

""")

user_table_create = ("""
create table if not exists users (
    user_id int NOT NULL primary key sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
diststyle all;
""")

song_table_create = ("""
create table if not exists songs (
    song_id varchar NOT NULL primary key sortkey,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration numeric NOT NULL,
    CONSTRAINT fk_artist
       FOREIGN KEY(artist_id) 
          REFERENCES artists(artist_id)
    

)
diststyle all;
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id varchar NOT NULL primary key sortkey,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric)
diststyle all;
""")

time_table_create = ("""
create table if not exists time (
    start_time timestamp NOT NULL primary key sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int, 
    weekday int
)
diststyle all;
""")



# STAGING TABLES

staging_events_copy = ("""

COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON AS {}

""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""

COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON AS 'auto'

""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES


songplay_table_insert = ("""

insert into songplays (
    start_time,
    user_id ,
    level ,
    song_id ,
    artist_id , 
    session_id ,
    location ,
    user_agent ) 
    
    select 
    
    timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
    
    se.userId as user_id,
    se.level as level,
    ss.song_id,
    ss.artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
    
    from staging_events se left join staging_songs ss on se.artist=ss.artist_name and se.length=ss.duration and se.song = ss.title
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) 
select distinct userId as user_id, firstName as first_name, lastName as last_Name, gender, level from staging_events where user_id <> NULL
order by user_id



""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id,year,duration) 
select distinct song_id, title, artist_id, year, duration from staging_songs order by song_id

""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) 
select distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude from staging_songs order by artist_id

""")


time_table_insert = ("""
insert into time ( start_time ,
    hour ,
    day ,
    week ,
    month ,
    year , 
    weekday )  
    
select a.start_time,
EXTRACT (HOUR FROM a.start_time) as hour , EXTRACT (DAY FROM a.start_time) as day,
EXTRACT (WEEK FROM a.start_time) as week, EXTRACT (MONTH FROM a.start_time) as month,

EXTRACT (YEAR FROM a.start_time) as year, EXTRACT (WEEKDAY FROM a.start_time) as weekday
from (select distinct timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time
        from staging_events se) a

order by start_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,  artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, artist_table_insert, song_table_insert, time_table_insert,songplay_table_insert]
