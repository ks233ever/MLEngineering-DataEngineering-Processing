**High level overview of the steps required to deploy a Spark EMR Cluster on AWS**

* Deploy the cluster via console or AWS CLI
  * console instructions: <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html>
* Update the master security group to allow SSH inbound access from your IP address
* Set up FoxyProxy for port forwarding
  * https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html
* After deployment, run ssh -i ~.aws/<your_pem_file.pem> -ND 8157 hadoop@<master_dns> to enable all URI links


This repo shows how to use Jupyter Notebooks within the EMR console which are connected to the cluster for data wrangling.
You can also run py scripts and access data directly from the cluster by SSH'ing into it.

**To do this :**

* ssh -i ~.aws/<your_pem_file.pem> hadoop@<master_dns>
* Once connected, there are two ways to access your data or scripts
  * Submit spark jobs directly from your S3 bucket
    * */usr/bin/spark-submit --master yarn s3://your_bucket_path/py_script.py*
  * Add a step to your cluster which copies data from your S3 bucket directly to the HDFS file system
    * <https://aws.amazon.com/premiumsupport/knowledge-center/copy-s3-hdfs-emr/>
    * Specify the ouput location on the cluster, for example, mine is *hdfs:////output-folder1/*
    * Can now access data files in *hdfs:////output-folder1/* and submit jobs via */usr/bin/spark-submit --master yarn hdfs:////output-folder1/py_script.py*
