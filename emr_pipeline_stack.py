from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
)
from constructs import Construct

class EmrPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Environment Variables
        source_bucket_name = "source-bucket"
        destination_bucket_name = "destination-bucket"

        # S3 Buckets
        source_bucket = s3.Bucket(self, "SourceBucket",
                                  bucket_name=source_bucket_name)
        destination_bucket = s3.Bucket(self, "DestinationBucket",
                                       bucket_name=destination_bucket_name)

        # SQS Queue
        queue = sqs.Queue(self, "EmrTriggerQueue")

        # Lambda Function to trigger EMR cluster
        emr_lambda = _lambda.Function(self, "EmrLambda",
                                      runtime=_lambda.Runtime.PYTHON_3_9,
                                      handler="lambda_function.lambda_handler",
                                      code=_lambda.Code.from_asset("lambda"),
                                      environment={
                                          "DESTINATION_BUCKET": destination_bucket.bucket_name
                                      })

        # Grant Lambda permissions to interact with S3 and EMR
        source_bucket.grant_read(emr_lambda)
        destination_bucket.grant_write(emr_lambda)

        # Notification from S3 to SQS
        source_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue))

        # Lambda trigger from SQS
        emr_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(queue)
        )

        # IAM Role for Lambda to interact with EMR
        emr_role = iam.Role(self, "EmrRole",
                            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                            managed_policies=[
                                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEMRFullAccessPolicy_v2")
                            ])
        emr_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["emr:RunJobFlow", "emr:DescribeStep", "emr:TerminateJobFlows"],
                resources=["*"]
            )
        )
