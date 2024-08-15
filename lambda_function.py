import boto3
import os
import json

s3_client = boto3.client('s3')
emr_client = boto3.client('emr')

def handler(event, context):
    # Get the bucket names from environment variables
    source_bucket_name = os.environ['SOURCE_BUCKET']
    target_bucket_name = os.environ['TARGET_BUCKET']

    # Process SQS messages
    for record in event['Records']:
        payload = json.loads(record['body'])
        object_key = payload['Records'][0]['s3']['object']['key']

        # Trigger EMR Cluster
        cluster_id = emr_client.run_job_flow(
            Name='MyEMRCluster',
            ReleaseLabel='emr-6.3.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': 'your-keypair-name',
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False
            },
            Steps=[{
                'Name': 'ProcessFiles',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        's3-dist-cp',
                        '--src', f's3://{source_bucket_name}/{object_key}',
                        '--dest', f's3://{target_bucket_name}/processed/'
                    ]
                }
            }],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f'Cluster created with ID: {cluster_id["JobFlowId"]}')
        }
