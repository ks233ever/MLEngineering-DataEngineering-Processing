Commonly repeated series of tasks in DAGs can be captured as reusable SubDAGs

**dag.py** 
  * Runs two subdags in parallel
    * The subdag contains a PostgresOperator to create a table, a S3ToRedshiftOperator to load data to the tables, and a custom HasRowsOperator which verifies data was inserted
    * See **subdag.py**
  * Performs a calculation
  
  
**DAG Tree**


**DAG Graph**
