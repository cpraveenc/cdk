import json
import boto3
import os

s3_client = boto3.client('s3')
emr_client = boto3.client('emr')

def lambda_handler(event, context):
    # Get the S3 bucket and key details from the event
    records = event['Records']
    
    for record in records:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        
        # Create EMR cluster
        response = emr_client.run_job_flow(
            Name='EMR-Cluster-For-Processing',
            ReleaseLabel='emr-6.5.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            Steps=[
                {
                    'Name': 'Process S3 Files',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            's3-dist-cp',
                            '--src=s3://{}/{}'.format(s3_bucket, s3_key),
                            '--dest=s3://{}'.format(os.getenv('DESTINATION_BUCKET'))
                        ]
                    }
                }
            ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('EMR cluster created and processing started.')
        }
