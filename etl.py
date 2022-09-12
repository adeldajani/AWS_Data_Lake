import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    
    """
    Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files which will be loaded on s3.
    Parameters
    ----------
    spark: session
          This is the spark session that has been created
    input_data: path
           This is the path to the song_data s3 bucket.
    output_data: path
            This is the path to where the parquet files will be written.
    """
    
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songs_table.dropDuplicates(subset = ['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])


    # extract columns to create artists table
    columns = ['artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    columns = [col + ' as ' + col.replace('artist_', '') for col in columns]
    artists_table = df.selectExpr('artist_id', *columns)
    artists_table.dropDuplicates(subset = ['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))



def process_log_data(spark, input_data, output_data):
    
    """
    Load data from log_data dataset and extract columns
    for users and time tables, reads both the log_data and song_data
    datasets and extracts columns for songplays table with the data.
    It writes the data into parquet files which will be loaded on s3.
    Parameters
    ----------
    spark: session
          This is the spark session that has been created
    input_data: path
           This is the path to the log_data s3 bucket.
    output_data: path
            This is the path to where the parquet files will be written.
    """
    
    
    # get filepath to log data file
    
    log_data = os.path.join(input_data, 'log_data',
                            '*', '*')

    # read log data file
    df = spark.read.json(log_data)

    
    # filter by actions for song plays
    df =  df.filter(df.page == 'NextSong') \
                   .select('ts', 'userId', 'level',
                           'song', 'artist',
                           'sessionId', 'location',
                           'userAgent')

    # extract columns for users table    
    user_table = df.select(
        col('firstName'), 
        col('lastName'), 
        col('gender'), 
        col('level'), 
        col('userId')
    ).dropDuplicates(subset = ['userId'])
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')


    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x:
                         str(int(int(x)/1000)))
    df = actions_df.withColumn('timestamp',
                                       get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = actions_df.select('datetime') \
                           .withColumn('start_time', actions_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates(subset = ['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,
                                          'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(f'{output_data}songplays/songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://datalakeproj99/sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
