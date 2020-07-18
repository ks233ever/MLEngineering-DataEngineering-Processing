## Dask Fargate Deployment Instructions

* Follow the top to bottom instructions [here](https://docs.google.com/document/d/1gaEZC-qEGDxEEXhFygFcNLasyMNwx21X2PhB5o2zgPM/edit?usp=sharing) to deploy a cluster with both local and Sagemaker access
  * Note that these steps cover DevOps proccesses ie VPC, Subnet, Security Group and Load Balancer creations


## Once Deployed, Connection Instructions

**Locally**

1. Ensure your IP address is within the DaskSecurityGroupIngress4 block in dask-template.yml
2. Create a conda environment using dask-cluster.yml
3. Adjust the cluster to your desired number of workers by running this line of code with the appropriate --desired-count 
  * !aws ecs update-service --service dask-workers --desired-count 2 --cluster Fargate-Dask-Cluster --region us-east-1
  * additional workers will take ~1 min to activate
4. Connect to the cluster by running 
  * from dask.distributed import Client
  * client = Client('dask-lb-18e7d35a6c8734fb.elb.us-east-1.amazonaws.com:8786')
  * client
 
 **SageMaker**
 
 1. See step 9 in top to bottom instructions
 2. Within your notebook instance, open up the terminal and run source activate daskcluster
 3. You can now create a conda_daskcluster notebook which will use the environment
  
*Notes:*
  * After finishing with the cluster, reduce the number of workers to 1. Always keep 1 so that the task definition which registers workers to the load balancer is not deleted.
  * __Never run client.shutdown()__, this will shut down the ECS Fargate cluster 
  
*Resources:*
 * https://blog.dask.org/2016/08/16/dask-for-institutions
 * https://docs.dask.org/en/latest/dataframe-best-practices.html
 
 **dask_template.yaml** is used to create the CloudFormation stack within AWS
