from aws_cdk import (
    Stack, aws_s3 as s3, aws_sqs as sqs, aws_lambda as _lambda,
    aws_s3_notifications as s3n, aws_iam as iam, aws_emr as emr
)
from constructs import Construct

class EmrPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Source S3 Bucket
        source_bucket = s3.Bucket(self, "SourceBucket")

        # Create Target S3 Bucket for processed data
        target_bucket = s3.Bucket(self, "TargetBucket")

        # Create SQS Queue
        queue = sqs.Queue(self, "S3ToSQSQueue")

        # Add S3 Event Notification to SQS for new object uploads
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue)
        )

        # Create IAM Role for Lambda to interact with S3 and EMR
        lambda_role = iam.Role(self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEMRFullAccessPolicy_v2"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # Create Lambda Function to trigger EMR cluster
        emr_lambda = _lambda.Function(self, "EMRClusterTriggerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("lambda"),
            role=lambda_role,
            environment={
                'SOURCE_BUCKET': source_bucket.bucket_name,
                'TARGET_BUCKET': target_bucket.bucket_name
            }
        )

        # Grant Lambda permissions to read from SQS and access the S3 buckets
        queue.grant_consume_messages(emr_lambda)
        source_bucket.grant_read(emr_lambda)
        target_bucket.grant_write(emr_lambda)

        # Set SQS as the trigger for Lambda
        emr_lambda.add_event_source(
            _lambda_event_sources.SqsEventSource(queue)
        )
