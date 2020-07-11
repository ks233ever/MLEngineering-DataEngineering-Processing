**Data Lake options on AWS include:**

* HDFS with Spark (AWS EMR)
   * See Spark_Deployment_and_EDA/Distributed_Deployment for steps on deploying the cluster, writing data to the HDFS file system, and directly SHH'ing into it

![alt text](images/HDFS_Spark.png?raw=true)

* S3 with Spark (AWS EMR)
  * See DataLake_S3_Spark.ipynb, a local example
  * See Spark_Deployment_and_EDA/Distributed_Deployment for steps on running an EMR notebook connected to your cluster, or directly SHH'ing into it

![alt text](images/S3_Spark.png?raw=true)

* S3 with Severless (AWS Athena)
  * In AWS Athena console, create table using S3 or Glue Crawler
  * Glue Crawler is a tool that creates crawlers which grab data from our S3 data store on a set frequency, and infers the schema
  * It outputs the data to a selected database
  * After running the crawler we can now query data from Athena-- this process is representative of a sophisticated schema on read

![alt text](images/Athena.png?raw=true)


**Spark_Schema_on_Read.ipynb** is an example of a transformation step in a Data Lake's ELT process-- where we store raw data and transform it at a later step.




