**Prior to running this program:**
* Create your connections via the Airflow UI
  * 'aws_credentials' is an Amazon Web Services connection type
  * 'redshift' is a Postgres connection type

**Looking at bikeshare customer data, this Airflow program:**

* Creates a trips table in Redshift
* Copies trip data from an S3 bucket to the table
* Creates an updated traffic analysis table from the newly created table
* Runs daily



**DAG Graph and Tree View**

![alt text](images/DAG_graph.png?raw=true)



![alt text](images/DAG_tree.png?raw=true)
