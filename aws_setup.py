from argparse import ArgumentParser, FileType
import boto3
from botocore.exceptions import ClientError
import configparser
import json
import logging
import pandas as pd

### Logging Handling ###

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)

def create_iam_role(config, iam):
    '''
    Create an IAM role with S3 Readonly access
    Use pre-defined IAM user credentials
    Return the new IAM role's ARN
    '''

    # create a new IAM role
    try:
        logger.info(f"Creating a new IAM role {config['IAM_ROLE']['IAM_ROLE_NAME']}...")
        dwhRole = iam.create_role(RoleName=config['IAM_ROLE']['IAM_ROLE_NAME'],
                                  Description = 'Allows Redshift clusters to call AWS services on your behalf',
                                  AssumeRolePolicyDocument=json.dumps(
                                        {'Statement': [
                                            {'Action': 'sts:AssumeRole',
                                             'Effect': 'Allow',
                                             'Principal': {'Service': 'redshift.amazonaws.com'}
                                             }
                                            ],
                                         'Version': '2012-10-17'
                                         })
        )
    except Exception as e:
        logger.error(e)

    # attach S3 Readonly policy
    try:
        logger.info(f"Attaching policy to role {config['IAM_ROLE']['IAM_ROLE_NAME']}...")
        iam.attach_role_policy(RoleName=config['IAM_ROLE']['IAM_ROLE_NAME'],
                               PolicyArn=config['IAM_ROLE']['S3_POLICY_ARN'])['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        logger.error(e)

    # get IAM role ARN
    try:
        logger.info(f"Get the ARN for role {config['IAM_ROLE']['IAM_ROLE_NAME']}...")
        roleArn = iam.get_role(RoleName=config['IAM_ROLE']['IAM_ROLE_NAME'])['Role']['Arn']
    except Exception as e:
        logger.error(e)

    return roleArn

def create_redshift_cluster(config, redshift, roleArn, ec2):
    """
    Create a RedShift cluster.
    Return an object with cluster descriptions
    """

    try:
        logger.info('Creating a new RedShift Cluster. Expect a few minutes to be completed...')
        response = redshift.create_cluster(
            # Hardware parameters
            ClusterType=config['CLUSTER']['CLUSTER_TYPE'],
            NodeType=config['CLUSTER']['NODE_TYPE'],
            NumberOfNodes=int(config['CLUSTER']['NUM_NODES']),

            # Identifiers & Credentials parameters
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            MasterUsername=config['CLUSTER']['DB_USERNAME'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],

            # Roles (for s3 access) parameters
            IamRoles=[roleArn]
        )

        # wait 1 hour to check for cluster availability
        # runs .Redshift.Client.describe_clusters() each delay marker
        redshift.get_waiter('cluster_available').wait(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            WaiterConfig={
                'Delay': 60, 
                'MaxAttempts': 60
            })
    except Exception as e:
        logger.error(e)

    # describe cluster
    # logic from prettyRedshiftProps() in Exercise 3.2
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'])['Clusters'][0]
    except Exception as e:
        logger.error(e)
    
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in cluster_props.items() if k in keysToShow]
    logger.info(pd.DataFrame(data=x, columns=["Key", "Value"]))

    # open an incoming TCP port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        logger.info(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['CLUSTER']['PORT']),
            ToPort=int(config['CLUSTER']['PORT'])
        )
    except Exception as e:
        logger.error(e)

    logger.info(f"Successfully created the RedShift Cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']}!")

    return cluster_props

def delete_cluster_iam(config, cluster_props, redshift, iam):
    '''
    Delete redshift cluster & IAM role
    Only runs with --delete flag when running the script
    '''
    
    # display cluster status
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in cluster_props.items() if k in keysToShow]
    logger.info(pd.DataFrame(data=x, columns=["Key", "Value"]))
    
    # delete cluster
    try:
        redshift.delete_cluster(
                ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
                SkipFinalClusterSnapshot=True
                )
        logger.info(f"Successfully deleted cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']}")
    except Exception as e:
        logger.error(e)

    # detach policy & remove IAM role
    try:
        iam.detach_role_policy(
                RoleName=config['IAM_ROLE']['IAM_ROLE_NAME'],
                PolicyArn=config['IAM_ROLE']['S3_POLICY_ARN']
                )
        iam.delete_role(RoleName=config['IAM_ROLE']['IAM_ROLE_NAME'])
        logger.info(f"Detached policy & removed IAM role {config['IAM_ROLE']['IAM_ROLE_NAME']}")
    except Exception as e:
        logger.error(e)

def main():

    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # collect inputs to enable cluster and IAM role deletion
    parser = ArgumentParser(description="Collect input for AWS set up")

    parser.add_argument(
        '--delete',
        action='store_true',
        help='Boolean argument to delete Redshift Cluster & IAM role, default as False (call the arg for True)'
    )

    args = parser.parse_args()
    
    # AWS client & resource set up
    iam = boto3.client('iam',
                       region_name=config['AWS_CREDS']['REGION_NAME'],
                       aws_access_key_id=config['AWS_CREDS']['IAM_USER_ACCESS_KEY_ID'],
                       aws_secret_access_key=config['AWS_CREDS']['IAM_USER_SECRET']
                       )

    redshift = boto3.client('redshift',
                            region_name=config['AWS_CREDS']['REGION_NAME'],
                            aws_access_key_id=config['AWS_CREDS']['IAM_USER_ACCESS_KEY_ID'],
                            aws_secret_access_key=config['AWS_CREDS']['IAM_USER_SECRET']
                            )

    ec2 = boto3.resource('ec2',
                         region_name=config['AWS_CREDS']['REGION_NAME'],
                         aws_access_key_id=config['AWS_CREDS']['IAM_USER_ACCESS_KEY_ID'],
                         aws_secret_access_key=config['AWS_CREDS']['IAM_USER_SECRET']
                         )


    s3 = boto3.resource('s3',
                        region_name=config['AWS_CREDS']['REGION_NAME'],
                        aws_access_key_id=config['AWS_CREDS']['IAM_USER_ACCESS_KEY_ID'],
                        aws_secret_access_key=config['AWS_CREDS']['IAM_USER_SECRET']
                        )

    roleArn = create_iam_role(config, iam)
    cluster_props = create_redshift_cluster(config, redshift, roleArn, ec2)

    # log cluster endpoint (host) & IAM role arn
    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    logger.info(f"DWH_ENDPOINT :: {DWH_ENDPOINT}")
    logger.info(f"DWH_ROLE_ARN :: {DWH_ROLE_ARN}")

    if args.delete:
        delete_cluster_iam(config, cluster_props, redshift, iam)

if __name__ == '__main__':
    main()
