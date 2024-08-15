#!/usr/bin/env python3
from aws_cdk import core as cdk
from pipeline_stack import PipelineStack

app = cdk.App()
PipelineStack(app, "PipelineStack")

app.synth()
