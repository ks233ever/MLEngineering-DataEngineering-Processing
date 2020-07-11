import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

print(config)

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Create a Spark session which is the the entry point for accessing Spark's SQL and DataFrame API.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    This function reads the songs JSON files from S3 and processes them with Spark. It reads the json file in as a dataframe. 
    Spark's Schema On Read functionality allows us to represent each of these as a table. It then writes these tables into parquet files.
    
    
    Parameters
    ----------
    spark: session
          The spark session that we create
    input_data: path
           The path to the songs_data s3 bucket
    output_data: path
            The path to where the parquet files will be written

    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("SELECT distinct song_id, title as song_title, artist_id, year, duration FROM songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs")
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")
    
    print('songs table and artist table written to parquet')


def process_log_data(spark, input_data, output_data):
    
    """
    This function loads the log_data dataset and filters by song play action on NextSong. It then extracts the relevant columns for the
    users and time tables. It reads the song_data dataset, joins to log_data, and extracts columns for songplays table.
    It writes the data to parquet files which will be loaded to S3.
    
    
    Parameters
    ----------
    spark: session
          The spark session that we create
    input_data: path
           The path to the log_data s3 bucket
    output_data: path
           The path to where the parquet files will be written

    """
    
    # noting here: trying out alternative method of manipulating the data vs using SQL API
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("logdata")

    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist','sessionId', 'location', 'userAgent')
    df.createOrReplaceTempView("logdata")
  
    print('filtered')
    
    # extract columns for users table    
    users_df = df.select('userId', 'firstName', 'lastName','gender', 'level').dropDuplicates()
    users_df.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    
    print('users to parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                        .withColumn('start_time', actions_df.datetime) \
                        .withColumn('hour', hour('datetime')) \
                        .withColumn('day', dayofmonth('datetime')) \
                        .withColumn('week', weekofyear('datetime')) \
                        .withColumn('month', month('datetime')) \
                        .withColumn('year', year('datetime')) \
                        .withColumn('weekday', dayofweek('datetime')) \
                        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')
    
    print('time_table to parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    # creating aliases for tables to use in the join
    
    df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    time_table = time_table.alias('timetable')
    
    # join log and song datasets
    joined_df = df.join(song_df, col('log_df.artist') == col(
        'song_df.artist_name'), 'inner')
    
    # create songplays table
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month'))
    
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'),'overwrite')
    
    print('songplay_table to parquet')


def main():
    
    """
    1. Create or retrieve the spark session.
    2. Read the song and log data from s3.
    3. Read in these datasets and transform them to a table.
    4. Write these tables to parquet files.
    5. Load the parquet files to s3.
    """
        
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakedend/project_output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
