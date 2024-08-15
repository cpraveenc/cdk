from aws_cdk import (
    core as cdk,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_iam as iam,
    aws_emr as emr,
    aws_events as events,
    aws_events_targets as targets,
)
import os

class PipelineStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Load environment variables
        input_bucket_name = os.getenv('INPUT_BUCKET_NAME')
        output_bucket_name = os.getenv('OUTPUT_BUCKET_NAME')
        lambda_timeout = int(os.getenv('LAMBDA_TIMEOUT', 300))
        
        # S3 Buckets
        input_bucket = s3.Bucket(self, "InputBucket", bucket_name=input_bucket_name)
        output_bucket = s3.Bucket(self, "OutputBucket", bucket_name=output_bucket_name)

        # SQS Queue
        queue = sqs.Queue(self, "FileProcessingQueue")

        # Lambda Function to Trigger EMR Cluster
        emr_lambda = _lambda.Function(
            self, 
            "EmrLambdaFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="emr_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=cdk.Duration.seconds(lambda_timeout),
            environment={
                "OUTPUT_BUCKET_NAME": output_bucket.bucket_name,
            }
        )

        # Permissions for Lambda to access S3 and EMR
        emr_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*", "elasticmapreduce:*"],
                resources=["*"]
            )
        )

        # S3 Notification to SQS
        input_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue)
        )

        # SQS Trigger for Lambda
        emr_lambda.add_event_source(_lambda_event_sources.SqsEventSource(queue))

        # IAM Role for EMR
        emr_role = iam.Role(self, "EMRRole", 
                            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
                            managed_policies=[
                                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceRole")
                            ])

        # EMR Cluster
        cluster = emr.CfnCluster(
            self, 
            "EmrCluster",
            name="DataProcessingCluster",
            release_label="emr-6.7.0",
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=1, instance_type="m5.xlarge"
                ),
                core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=2, instance_type="m5.xlarge"
                ),
                ec2_key_name="your-ec2-key-name"
            ),
            job_flow_role=emr_role.role_name,
            service_role=emr_role.role_name
        )

        # Trigger EMR Cluster completion event to handle processing completion
        emr_completion_rule = events.Rule(
            self, "EMRCompletionRule",
            event_pattern={
                "source": ["aws.emr"],
                "detail-type": ["EMR Cluster State Change"],
                "detail": {
                    "state": ["TERMINATED"]
                }
            }
        )

        emr_completion_rule.add_target(
            targets.LambdaFunction(emr_lambda)
        )

        # Grant permissions
        input_bucket.grant_read(emr_lambda)
        output_bucket.grant_write(emr_lambda)
