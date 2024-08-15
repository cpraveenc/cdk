#!/usr/bin/env python3
import os
import aws_cdk as cdk
from emr_pipeline_stack import EmrPipelineStack

app = cdk.App()
EmrPipelineStack(app, "EmrPipelineStack")

app.synth()
