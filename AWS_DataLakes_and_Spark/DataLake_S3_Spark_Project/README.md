**The purpose of this project is to:**

* Move Sparkify's-- a music streaming startup-- Data Warehouse to a Data Lake
    * Build an ETL pipeline to extract Sparkify's data on S3
    * Process the data into analytics tables using Spark
    * Load the data back to S3 as a set of dimensional tables in parquet format
    * Deploy this Spark process on a cluster via AWS
    
* Enable Sparkify to discover insights re what songs their users are listening to

**Database Schema Design:**

* This is a star schema model which will allow Sparkify to easily query their users' data
    * The Fact table is songplays:
        * Contains records in log data associated with song plays
        * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
    * Dimension tables include:
        * Users
            * Users in the app
            * user_id, first_name, last_name, gender, level
        * Songs
            * Songs in the music database
            * song_id, title, artist_id, year, duration
        * Artists
            * Artists in the music database
            * artist_id, name, location, lattitude, longitude
        * Time
            * Timestamps of records in songplays, broken down into units
            * start_time, hour, day, week, month, year, weekday
            
**ETL.py:**

* Automated pipeline which:
    * Creates a Spark session
    * Reads Sparkify's song and log data from s3
    * Processes the data into analytics tables using Spark
    * Load the data back to S3 as a set of dimensional tables in parquet format**The purpose of this project is to:**

* Move Sparkify's-- a music streaming startup-- Data Warehouse to a Data Lake
    * Build an ETL pipeline to extract Sparkify's data on S3
    * Process the data into analytics tables using Spark
    * Load the data back to S3 as a set of dimensional tables in parquet format
    * Deploy this Spark process on a cluster via AWS
    
* Enable Sparkify to discover insights re what songs their users are listening to

**Database Schema Design:**

* This is a star schema model which will allow Sparkify to easily query their users' data
    * The Fact table is songplays:
        * Contains records in log data associated with song plays
        * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
    * Dimension tables include:
        * Users
            * Users in the app
            * user_id, first_name, last_name, gender, level
        * Songs
            * Songs in the music database
            * song_id, title, artist_id, year, duration
        * Artists
            * Artists in the music database
            * artist_id, name, location, lattitude, longitude
        * Time
            * Timestamps of records in songplays, broken down into units
            * start_time, hour, day, week, month, year, weekday
            
**ETL.py:**

* Automated pipeline which:
    * Creates a Spark session
    * Reads Sparkify's song and log data from s3
    * Processes the data into analytics tables using Spark
    * Load the data back to S3 as a set of dimensional tables in parquet format
