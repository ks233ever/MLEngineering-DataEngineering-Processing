Demonstrate the functionality of custom operators in Airflow

**This program:**

* Creates a stations table and trips table in Redshift
* Loads data from S3 to the tables
  * Uses a custom S3toRedshiftOperator()
  * See operators/s3_to_redshift.py
* Performs a data quality check to ensure rows were inserted into the tables
  * Uses a custom HasRowsOperator()
  * See operators/has_rows.py
  
  
