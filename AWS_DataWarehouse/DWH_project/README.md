**Purpose of this project is to:**

* Move Sparkify's-- a music streaming startup-- data to the cloud. 
* Build an ETL pipeline that will:
    * Extract Sparkify's json data from S3
    * Stage it in Redshift
    * Transorm it into a set of dimensional tables 
 
 Once in dimensional tables Sparkify can easily query the data to find insights re what songs their users are listening to. 
 
 * Create a star schema optimized for queries on song play analysis
     * Staging Tables:
         * events_stage
         * songs_stage
     * Fact Table:
         * Songplays-- records songplay associated data
         * Records with page 'NextSong' include songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
     * Dimension Tables:
         * Users-- Sparkify users-- user_id, first_name, last_name, gender, level
         * Songs-- Songs in the music database-- song_id, title, artist_id, year, duration
         * Artists-- Artists in the music database-- artist_id, name, location, lattitude, longitude
         * Time-- Timestamps for records in songplays broken down into units-- start_time, hour, day, week, month, year, weekday
         
 **Steps:**
 
 * Run create_tables.py to create the relevant tables
 * Run etl.py which:
     * Extracts Sparkify songs and user data from S3
     * Transforms the data using the staging tables
     * Loads the data into a set of dimensional tables
