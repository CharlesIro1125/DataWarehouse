import boto3
import time
import pandas as pd
import json
import configparser
from redshift import redshift, iam, ec2


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


# the redshift cluster and role identifier
CL_ID = config["DWH"]["DWH_CLUSTER_ID"]
ROLE_NAME = config["DWH"]["ROLE_NAME"]


def main():
    
    """"
        Description: 
        
            This function deletes the created redshift cluster 
            and also detaches the role policy before deleting the
            role.
    """
    
    
    
    try:
        
        try:
            
            redshift.delete_cluster(ClusterIdentifier = CL_ID, SkipFinalClusterSnapshot = True)
    
            i = 0
            myClusterProps = redshift.describe_clusters(ClusterIdentifier = CL_ID)['Clusters'][0]["ClusterStatus"]
            
            while myClusterProps == 'deleting':
                time.sleep(10) 
                if i == 0:
                    print('Deleting a Redshift Cluster with ID = {}  ....'.format(CL_ID))
                i += 1
                try:
                    
                    myClusterProps = redshift.describe_clusters(ClusterIdentifier = CL_ID)['Clusters'][0]["ClusterStatus"]
                except:
                    print('deleting cluster')
                    break          
            print('Redshift Cluster Deleted')    
        except:
            print('No Redshift cluster found')   
        try:
            
            iam.detach_role_policy(RoleName = ROLE_NAME, PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

            iam.delete_role(RoleName = ROLE_NAME)
        except:
            print('iam client Not found')    
    except:
        print('Create a Redshift cluster with iam policy')



if __name__ == "__main__":
    main()