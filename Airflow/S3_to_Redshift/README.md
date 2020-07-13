**Looking at bikeshare customer data, this Airflow program:**

* Creates a trips table in Redshift
* Copies trip data from an S3 bucket to the table
* Creates an updated traffic analysis table from the newly created table
* Runs daily
