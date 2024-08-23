#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import Tags

from glue_pipeline.glue_pipeline_stack import GluePipelineStack
from lambda_pipeline.lambda_pipeline_stack import LambdaPipelineStack
from data_deployment.data_deployment_stack import DataDeploymentstack
from glue_database.glue_database_stack import GlueDatabaseStack


env_USA = cdk.Environment(account="143176219551", region="us-west-2")
app = cdk.App()

glue_pipeline_stack = GluePipelineStack(app, "GluePipelineStack", env=env_USA)
lambda_pipeline_stack = LambdaPipelineStack(app, "LambdaPipelineStack", env=env_USA)
data_deployment_stack = DataDeploymentstack(app, "DataDeploymentStack", env=env_USA)
glue_database_stack = GlueDatabaseStack(app, "GlueDatabaseStack", env=env_USA)

data_deployment_stack.add_dependency(lambda_pipeline_stack)
glue_database_stack.add_dependency(glue_pipeline_stack)

Tags.of(app).add("ProjectOwner", "Alex-Clark")
Tags.of(app).add("Project", "Big-Data-Pipeline")
app.synth()
