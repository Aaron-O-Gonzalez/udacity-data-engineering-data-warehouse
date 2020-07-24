import configparser

'''Parameters used to define AWS configuration parameters and dataset files from S3 bucket'''
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
LOG_DATA= config.get('S3', 'LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

'''Drop tables in case there are pre-existing tables of the same name'''

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays "
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

'''SQL Queries to generate tables'''
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                 eventId BIGINT IDENTITY(0,1),
                                 artistName VARCHAR,
                                 auth VARCHAR,
                                 firstName VARCHAR,
                                 gender VARCHAR(1),
                                 itemInSession INT NOT NULL,
                                 lastName VARCHAR,
                                 length FLOAT,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method VARCHAR,
                                 page VARCHAR,
                                 registration VARCHAR,
                                 sessionId INT,
                                 song VARCHAR,
                                 status INT,
                                 ts BIGINT,
                                 userAgent VARCHAR,
                                 userId INT)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 num_songs INT,
                                 artist_id VARCHAR,
                                 artist_latitude VARCHAR,
                                 artist_longitude VARCHAR,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration FLOAT,
                                 year SMALLINT)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplayId BIGINT IDENTITY(0,1) PRIMARY KEY,
                            startTime TIMESTAMP NOT NULL,
                            userId INT NOT NULL,
                            level VARCHAR NULL,
                            songId VARCHAR NOT NULL,
                            artistId VARCHAR NOT NULL,
                            sessionId INT NULL,
                            location VARCHAR NULL,
                            userAgent VARCHAR NULL)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        userId INT PRIMARY KEY,
                        firstName VARCHAR NULL,
                        lastName VARCHAR NULL,
                        gender VARCHAR NULL,
                        level VARCHAR NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        songId VARCHAR PRIMARY KEY,
                        title VARCHAR NULL,
                        artistId VARCHAR NULL,
                        year SMALLINT NULL,
                        duration FLOAT NULL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artistId VARCHAR PRIMARY KEY,
                        artistName VARCHAR NULL,
                        artistLocation VARCHAR NULL,
                        artistLatitude VARCHAR NULL,
                        artistLongitude VARCHAR NULL)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        startTime TIMESTAMP PRIMARY KEY,
                        hour SMALLINT,
                        day SMALLINT,
                        week SMALLINT,
                        month SMALLINT,
                        year SMALLINT,
                        weekday SMALLINT)""")

'''SQL Queries to create staging tables for raw data'''

staging_events_copy = ("""COPY staging_events FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS json {}
                          region 'us-west-2';
                          """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         FORMAT as json 'auto'
                         region 'us-west-2';
                         """).format(SONG_DATA, ARN)

'''Inser values from staging tables into respective fields of fact/dimension tables'''

songplay_table_insert = ("""INSERT INTO songplays(
                            startTime,
                            userId,
                            level,
                            songId,
                            artistId,
                            sessionId,
                            location,
                            userAgent)
                            
                            SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
                                   se.userId as userId,
                                   se.level as level,
                                   ss.song_id as songId,
                                   ss.artist_id as artistId,
                                   se.sessionId as sessionId,
                                   se.location as location,
                                   se.userAgent as userAgent
                            
                            FROM staging_events AS se
                            JOIN staging_songs AS ss
                            ON (se.artistName=ss.artist_name)
                            
                            WHERE se.page='NextSong';""")

user_table_insert = ("""INSERT INTO users(userId, 
                                          firstName,
                                          lastName,
                                          gender,
                                          level)
                                          
                        SELECT DISTINCT se.userId,
                               se.firstName,
                               se.lastName,
                               se.gender,
                               se.level
                        
                        FROM staging_events as se
                        
                        WHERE se.page='NextSong';""")

song_table_insert = ("""INSERT INTO songs(songId,
                                          title,
                                          artistId,
                                          year,
                                          duration)  
                                         
                        SELECT DISTINCT ss.song_id as songId,
                               ss.title as title,
                               ss.artist_id as artistId,
                               ss.year as year,
                               ss.duration as duration
                        
                        FROM staging_songs as ss;""")

artist_table_insert = ("""INSERT into artists(artistId,
                                              artistName,
                                              artistLocation,
                                              artistLatitude,
                                              artistLongitude)
                          
                          SELECT DISTINCT ss.artist_id as artistId,
                                    ss.artist_name as artistName,
                                    ss.artist_location as artistLocation,
                                    ss.artist_latitude as artistLatitude,
                                    ss.artist_longitude as artistLongitude
                          
                          FROM staging_songs as ss;""")

time_table_insert = ("""INSERT into time(startTime,
                                         hour,
                                         day,
                                         week, 
                                         month,
                                         year, 
                                         weekday)
                        
                        SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
                        EXTRACT(hour FROM start_time) as hour,
                        EXTRACT(day FROM start_time)  as day,
                        EXTRACT(week FROM start_time) as week,
                        EXTRACT(month FROM start_time) as month,
                        EXTRACT(year FROM start_time) as year,
                        EXTRACT(weekday FROM start_time) as weekday
                        
                        FROM staging_events as se 
                        WHERE se.page='NextSong';""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]