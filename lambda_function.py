import boto3
import os

emr_client = boto3.client('emr')
s3_client = boto3.client('s3')

def handler(event, context):
    # Retrieve the output bucket name from environment variables
    output_bucket_name = os.getenv('OUTPUT_BUCKET_NAME')
    
    # Here, you will trigger your EMR cluster or handle completion
    if event.get("detail-type") == "EMR Cluster State Change" and event["detail"]["state"] == "TERMINATED":
        # Handle EMR Cluster completion
        # This is where the processed files would be moved to the output S3 bucket
        s3_client.put_object(Bucket=output_bucket_name, Key="result.csv", Body=b"Processed data...")
        return {
            'statusCode': 200,
            'body': 'Processing complete and result stored in output S3 bucket.'
        }
    
    # Logic for starting the EMR Cluster
    response = emr_client.run_job_flow(
        Name='MyJobFlow',
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
            'Ec2KeyName': 'your-ec2-key-name',
            'KeepJobFlowAliveWhenNoSteps': False,
        },
        Steps=[],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        ReleaseLabel='emr-6.7.0',
        LogUri=f's3://{output_bucket_name}/emr-logs/'
    )
    
    return {
        'statusCode': 200,
        'body': f'EMR cluster created with id: {response["JobFlowId"]}'
    }
