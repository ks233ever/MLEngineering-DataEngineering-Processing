Demonstrate the functionality of custom operators in Airflow

**custom_operators_in_airflow.py:**

* Creates a stations table and trips table in Redshift
* Loads data from S3 to the tables
  * Uses a custom S3toRedshiftOperator()
  * See plugins/operators/s3_to_redshift.py
* Performs a data quality check to ensure rows were inserted into the tables
  * Uses a custom HasRowsOperator()
  * See plugins/operators/has_rows.py
  
  Tree representation of DAG:
  
  ![alt text](images/custom_op.png?raw=true)
