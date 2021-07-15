## Table of contents
* [Project Introduction](#project-intro)
* [Technologies](#technologies)
* [Database Schema](#dbschema)
* [ETL Pipeline](#etl)
* [Steps to create table & ETL process](steps)

## Project Introduction

This project is designed to help a startup company -- Sparkify that has grown their user base and song database to move their processses and data onto the cloud. Currently they have the data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ETL pipeline that extracts Sparkify's data from S3, process them using spark, and loads the data back into S3 as a set of dimensional tables for Sparkify's analytics team to continue finding insights in what songs their users are listening to.


	
## Technologies
Project is created with:
* configparser
* datetime
* pyspark

	
## Database Schema

#### Project Datasets
* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data


Using the song and log datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
* songplays - records in log data associated with song plays i.e. records with page NextSong.
   fields include: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
* users - users in the app
   fields include: user_id, first_name, last_name, gender, level
* songs - songs in music database
   fields include: song_id, title, artist_id, year, duration
* artists - artists in music database
   fields include: artist_id, name, location, latitude, longitude
* time - timestamps of records in songplays broken down into specific units
   fields include: start_time, hour, day, week, month, year, weekday



## ETL Pipeline

#### File location
* etl.py
* dl.cfg

#### ETL Steps
1. Create & Launch EMR cluster on AWS
2. Create Spark Session using AWS access credential in dl.cfg  
3. Process Songs data to create songs and artists tables & load the tables to S3 directory
4. Process Logs data to create time, users, songplays tables & load the tables to S3 directory

#### Create Spark Session
Function defined in etl.py: create_spark_session:
```
    def create_spark_session():
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        return spark
```
#### Process Songs data
Function defined in etl.py: process_song_data:

Steps:  
- Define the songs data input path  
- Load the input the data & create Spark dataframe  
- Select the columns needed for songs & artists table from the dataframe & drop the duplicate records in each table  
- Load each table as parquet format to the defined directory in S3 with partition key defined  
 

#### Process Logs data
Function defined in etl.py: process_log_data:

Steps:
- Define the logs data input path  
- Load the input the data & create Spark dataframe with filter by actions for song plays  
- Extract columns for users table & drop the duplicates & write the users table as parquet format to the defined directory in S3 with partition key (user_id) defined
- Create timestamp & datetime columns with user defined function
- Extract columns to create time table & drop the duplicates & write the time table as parquet format to the defined directory in S3 with partition key (year, month) defined
- Read in song data to use for songplays table & extract columns from joined song and log datasets to create songplays table 
- Write songplays table to parquet files partitioned by year and month to the defined directory in S3


## Steps to create table & ETL process

* ssh to connect the running EMR cluster & run etl.py for ETL process



