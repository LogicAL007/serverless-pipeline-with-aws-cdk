from aws_cdk import (
    Stack,
    aws_s3_deployment as s3deploy,
    aws_s3 as s3,
)
from constructs import Construct
from decouple import config

BUCKET_NAME = config("BUCKET_NAME")

# Deploys data to S3 which triggers the lambda
class DataDeploymentstack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket.from_bucket_name(self, "bucket", BUCKET_NAME)
    
        data_deployment = s3deploy.BucketDeployment(self, "DeployData",
                sources=[s3deploy.Source.asset("./data/")],
                destination_bucket=bucket,
                destination_key_prefix="data/",
                prune=False
            )