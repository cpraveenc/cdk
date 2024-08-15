#!/usr/bin/env python3
import os
import aws_cdk as cdk
from my_pipeline_stack import MyPipelineStack

app = cdk.App()

# Load environment variables from .env or directly from environment
env = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION'))

MyPipelineStack(app, "MyPipelineStack", env=env)

app.synth()
