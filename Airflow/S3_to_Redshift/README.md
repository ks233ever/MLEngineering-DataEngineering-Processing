**Prior to running this program:**
* Create your connections via the Airflow UI
  * 'aws_credentials' is an Amazon Web Services connection type
  * 'redshift' is a Postgres connection type

**Looking at bikeshare customer data, this Airflow program:**

* Creates a trips table in Redshift
* Copies trip data from an S3 bucket to the table
* Creates an updated traffic analysis table from the newly created table
* Runs daily

**Some things to note:**
* If your DAG includes a start_time in the past, Airflow will backfill your data, i.e. the DAG is run as many x as there are schedule intervals between start_time and the current date or specified end_date

* Within the location_traffic_task PostgresOperator, you could partition your data on time, so the analysis is run for the time period of the DAG
  * You could achieve this by including the prev_ds (previous DAG run) and next_ds (next DAG run) context variables in your SQL formatted string like so
  * WHERE end_time > {{{{ prev_ds }}}} AND end_time < {{{{ next_ds }}}}


**DAG Graph and Tree View**

![alt text](images/DAG_graph.png?raw=true)



![alt text](images/DAG_tree.png?raw=true)


