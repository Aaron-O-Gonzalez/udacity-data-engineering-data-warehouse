# Sparkify Data Warehouse

The enclosed code creates a Postgresql database through an Amazon Web Services (AWS) account. A Redshift cluster is created, and assigned a read-only identity and access management (IAM) role for generating information schema from two datasets in an S3 bucket. The following are user-required parameters that should be populated in the **dwh.cfg** file:

**CLUSTER**

HOST='redshift_endpoint'
DB_NAME='db_name'
DB_USER= 'db_user'
DB_PASSWORD='db_pass'
DB_PORT= 5439

**IAM**
ARN=IAM_ROLE

**S3**
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

# Staging Tables
The two datasets, LOG_DATA and SONG_DATA, are json files which are copied into separate tables called **staging_events** and **staging_songs**, respectively. 

**Staging_events**
This table is comprised of the following fields: artistName, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

**Staging_songs**
This table is comprised of the following fields:num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year


# Star Schema
The staging tables are used for generating the **songplays**, **users**, **songs**, **artists**, and **time** tables.

**FACT TABLE**
**songplays**
This fact table is composed of the following fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. 

**DIMENSION TABLES**
**users**
This dimension table is composed of user_id, first_name, last_name, gender, level

**songs**
This dimension table is composed of song_id, title, artist_id, year, duration

**artists**
This dimension table is composed of artist_id, name, location, latitutde, longitude

**time**
This dimension table is composed of timestamp, hour, day, week, month, year, weekday

#ETL Pipeline
The dimension tables are constructed using the staging_events and staging_songs tables. Specifically,
users and time are constructed using the staging_events table, whereas songs and artists are constructed from song_data. Both the **staging_events** and **staging_songs** table share the artist name field, which is used to inner join the two tables and insert data into the **songplays** table. 

The **songplays** table identifies a specific point in time that may be of interest in the query, i.e. 

> SELECT * FROM songplays
> WHERE songplay_id =50

The fields returned are atomic values which can then be used to be used to query the dimension tables. For example, if the songplay_id of 50 returns a value for user_id of 20, we can then find out more information about the user from the users table:

> SELECT * FROM users
> WHERE user_id = 20
