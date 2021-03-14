import boto3
import time
import pandas as pd
import json
import configparser


# configuration file 
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


# aws key and secret key
KEY = config.get("ACCESS","KEY")

SECRET = config.get("ACCESS","SECRET")




# instantiating the redshift client, iam client and the ec2 virtual private cloud
#using key/secret key credentials for authentication. 

iam = boto3.client('iam',aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET,
                     region_name = 'us-west-2'
                     )


redshift = boto3.client('redshift',
                       region_name = "us-west-2",
                       aws_access_key_id = KEY,
                       aws_secret_access_key = SECRET
                       )


ec2 = boto3.resource('ec2',
                       region_name = "us-west-2",
                       aws_access_key_id = KEY,
                       aws_secret_access_key = SECRET
                       )


def create_role(iam):
    
    """
        Description: 
        
            This function creates an identity and access management (iam)
            client role and assign roles or duties to various aws resource.
            AmazonS3ReadOnlyAccess role is assigned. The role identifier
            name that identifies this role is read from the config file
            as string.
              
        Arguments:
        
            iam:  identity and access management (iam) client 
               
        Returns:
            roleArn: the role resource name for AmazonS3ReadOnlyAccess
    """
    
    try:
        
        role_response = iam.create_role(
                        Path = '/',
                        RoleName = config["DWH"]["ROLE_NAME"],
                        Description = 'Allows Redshift clusters to call AWS services on your behalf.',
                        AssumeRolePolicyDocument = json.dumps(
                            {'Statement': [{'Action': 'sts:AssumeRole',
                               'Effect': 'Allow',
                               'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                 'Version': '2012-10-17'}))
        
        iam.attach_role_policy(RoleName = config["DWH"]["ROLE_NAME"],
                              PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                              )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName = config["DWH"]["ROLE_NAME"])['Role']['Arn']
    except Exception as e:
        print(e)      
    return roleArn
    

    
def create_cluster(redshift, roleArn):
    """
        Description: 
        
            This function creates a redshift cluster with a default database
            and assigns a role to the cluster.The database name, user name,
            user password, cluster type, number of nodes,node type and 
            cluster identifier are provided as string read from the config
            file (dwh.cfg).
              
        Arguments:
        
            redshift:  A redshift client use for creating the cluster
            roleArn : The role resource name to enable read only action from
                    an S3 bucket.
               
        Returns:
            None
            
    """

    
    
    try:
        
        response = redshift.create_cluster(DBName = config["CLUSTER"]["DB_NAME"],
                                      ClusterIdentifier = config["DWH"]["DWH_CLUSTER_ID"],
                                      ClusterType = config["DWH"]["DWH_CLUSTER_TYPE"],
                                      NodeType = config["DWH"]["DWH_NODE_TYPE"],
                                      MasterUsername = config["CLUSTER"]["DB_USER"],
                                      MasterUserPassword = config["CLUSTER"]["DB_PASSWORD"],
                                      NumberOfNodes = int(config["DWH"]['DWH_NUM_NODES']),
                                      IamRoles = [roleArn]
                                      ) 
    except Exception as e:
        print(e)
    

def create_vpc_port(ec2, vpc_id): 
    
    """
        Description: 
        
            This function creates an ec2 virtual private cloud using the
            redshift cluster virtual private cloud identifier and allows 
            for inbound TCP from outside the virtual private cloud. The 
            redshift cluster virtual private cloud identifier provides
            information on the virtual private cloud the 
            redshift was created.
              
        Arguments:
        
            ec2: An elastic compute cloud (ec2) instance for accessing a
                virtual private cloud instance.
            
            vpc_id : Redshift cluster virtual private cloud identifier
               
        Returns:
            None
    """
        
    
    
    try:
        
        vpc = ec2.Vpc(id = vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(
            GroupName = defaultSg.group_name,
            CidrIp = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort = int(config["CLUSTER"]["DB_PORT"]),
            ToPort = int(config["CLUSTER"]["DB_PORT"])
            )
        print('Vpc port is open')        
    except:
        print('Vpc port is open, peering rule already exists')
    
    
    
def main():
    
    """
        Description: 
        
            This function creates a role using the create_role method
            and applies the returned role Arn value to create a cluster
            with AmazonS3ReadOnlyAccess permission.
            After the cluster is created it enables TCP communication
            from external route with the create_vpc_port method.
            
            The Redshift cluster endpoint and the role Arn is written 
            into the config file.
              
    """
    
    
    

    CLUSTER_ID = config["DWH"]["DWH_CLUSTER_ID"]
    
    roleArn = create_role(iam)
    
    create_cluster(redshift, roleArn)


    try:
        
        myClusterProps = redshift.describe_clusters(ClusterIdentifier = CLUSTER_ID)['Clusters'][0]
        
        i = 0
        
        while myClusterProps["ClusterStatus"] != 'available': 
            time.sleep(7)
            if i == 0:
                print('Creating a Redshift Cluster with ID = {}  ......'.format(CLUSTER_ID))
            i += 1
            myClusterProps = redshift.describe_clusters(ClusterIdentifier = CLUSTER_ID)['Clusters'][0]
        print('Redshift Cluster available')
        vpc_id = myClusterProps['VpcId']
        create_vpc_port(ec2, vpc_id)
    except Exception as e:
        print(e)
    
    DWH_ENDPOINT = myClusterProps["Endpoint"]['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    
    config['CLUSTER']['host'] = DWH_ENDPOINT
    config['IAM_ROLE']['arn'] = DWH_ROLE_ARN

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
    print(" Redshift endpoint : {} \n Role arn : {}".format(DWH_ENDPOINT, DWH_ROLE_ARN))
    print('Your Redshift cluster is ready for connection')

    
    
    
    
if __name__ == "__main__":
    main()