import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # create a temp view table for sql query
    df.createOrReplaceTempView("songs")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates().orderBy(["year","artist_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_path=output_data+"/songs"
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(songs_output_path)

    # extract columns to create artists table 
    artists_table = spark.sql('''
        select distinct 
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
        from 
        songs order by artist_id
        ''')
    
    
    # write artists table to parquet files
    artists_output_path=output_data+"/artists"
    artists_table.write.partitionBy("artist_id").mode("overwrite").parquet(artists_output_path)

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
     
    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays
    df = df.filter(df['page']=="NextSong")
    
    # create a temp view table for sql query
    df.createOrReplaceTempView("logs_base")
    
    # extract columns for users table    
    users_table = spark.sql('''
    select distinct userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level from logs_base
    order by user_id
    ''')
    
    # write users table to parquet files
    users_output_path=output_data+"/users"
    users_table.write.partitionBy("user_id").mode("overwrite").parquet(users_output_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000), IntegerType() )
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
                                                                               
    df = df.withColumn("datetime", get_datetime("timestamp"))
    
    # extract columns to create time table
    time_table = df.select(
    col('timestamp').alias('start_time'),
                       hour('datetime').alias('hour'),
                       dayofmonth('datetime').alias('day'),
                       weekofyear('datetime').alias('week'),
                       month('datetime').alias('month'),
                       year('datetime').alias('year'),
                       date_format('datetime','E').alias('weekday')
    ).dropDuplicates().orderBy(["year","month"])
    
    # write time table to parquet files partitioned by year and month
    time_output_path=output_data+"/time"
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(time_output_path)

    # read in song data to use for songplays table
    song_data_path=input_data+"song_data/*/*/*/*.json"  
       
    # read song data file
    song_df = spark.read.json(song_data_path)
   
    # create a temp view table for sql query
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        select 
        row_number() over (order by l.timestamp) as songplay_id,
        l.timestamp as start_time,
        year(l.datetime) as year,
        month(l.datetime) as month,
        l.userId as user_id,
        l.level as level,
        s.song_id,
        s.artist_id,
        l.sessionId as session_id,
        l.location,
        l.userAgent as user_agent

        from logs l join songs s on l.artist=s.artist_name and l.length=s.duration and l.song = s.title
        order by year, month
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_ouput_path=output_data+"/songplays"
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(songplays_ouput_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mydatalakeproject/Sparkify"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
