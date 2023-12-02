# Udacity NanaoDegree DataLakesProject

**Introduction**

Sparikfy a music streaming startup has grown its user base along with songs database. As a result they are facing challenges with their existing datawarehouse. They wish to move to the data lake for efficciently managing their users and songs data. Currently their data resides in S3 inside the directory in JSON format as logs on user activity on the app along with a directory with JSON metadata on the songs in their app. 

As part of data engineering we need to build an ETL pipeline that extracts data from S3, processes it using Spark, and loads the data back into S3 as set of dimensional tables and fact tables. Which will help their analytics team to continue finding insights in what songs their users are listening to.

**Data Sets Used for Data Input**

Dataset consists of two JSON files present at below location:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data


**This project contains the following files which are required to execute it:**

1. dl.cfg: This file conatins details of AWS keys.
2. etl.py: This is the main file that will be executed as etl pipeline.
3. README.md: This file contains the overview of the project, explaining its objectives, different modules required to run it and how it can be run or reused.

**Steps to run the etl pipeline**

etl.py file:

1. It reads the credentials from the dl.cfg file
2. Loads the Song and Log data from the S3 location: s3://udacity-dend/song_data and s3://udacity-dend/log_data
3. Processes the data using spark
4. Loads it back to the S3 bucket: sparkify-dend-data-output/


**Tables Created**

Five tables are created as part of this project:

1. Users: This is the dimension table having following columns: user_id, first_name, last_name, gender, level
2. songs: This is the dimension table having following columns: song_id, title, artist_id, year, duration
3. artists: This is the dimension table having following columns: artist_id, name, location, lattitude, longitude
4. time: This is the dimension table having following columns: start_time, hour, day, week, month, year, weekday
5. songplays: This is the fact table having following columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. records in log data associated with song plays i.e. records with page NextSong
