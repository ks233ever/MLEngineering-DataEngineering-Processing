This project uses Airflow to create a data pipeline that moves source data in S3 into a Redshift DWH
* Stage events and songs tables
* Load the songplays fact table
* Load dimension tables
* Run data quality checks

This project uses custom operators as defined in the plugins/operators
* Stage Operator
* Fact table Operator
* Dimension table Operator
* Data Quality Operator 

**DAG Graph**

![alt text](images/airflow_p.png?raw=true)
