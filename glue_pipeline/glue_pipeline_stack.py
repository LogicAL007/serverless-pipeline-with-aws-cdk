from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_glue as glue,
    CfnTag,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct
from decouple import config

#Set variables from environment
JDBC_CONNECTION_STRING = config("JDBC")
RDS_VPC_ID = config("RDS_VPC_ID")
RDS_SUBNET_ID = config("RDS_SUBNET_ID")
ROUTE_TABLE_ID = config("ROUTE_TABLE_ID")
AVAILABILITY_ZONE=config("AZ")
RDS_USERNAME = config("RDS_USERNAME")
RDS_PASSWORD = config("RDS_PASSWORD")
BUCKET_NAME = config("BUCKET_NAME")
DATABASE_NAME = config("DATABASE_NAME")

RDS_SECURITY_GROUP_IDS = [
    config("SG1"),
    config("SG2"),
    config("SG3"),
    config("SG4"),
]

TAGS = [
    CfnTag(key="ProjectOwner",value="Alex-Clark"),
    CfnTag(key="ProjectName",value="Big-Data-Pipeline")
]

class GluePipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Glue permissions can be difficult to configure, it"s easiest to grand admin access
        glue_role = iam.Role(self, "GlueExecutorRole", assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"))

        # We need to deploy the Glue Job script to s3 in order to assign it to a job
        bucket = s3.Bucket.from_bucket_name(self, "bucket", BUCKET_NAME)
        script_deployment = s3deploy.BucketDeployment(self, "DeployGlueScript",
            sources=[s3deploy.Source.asset("./glue_pipeline/scripts/")],
            destination_bucket=bucket,
            destination_key_prefix="scripts/",
            prune=False
        )

        #################################################################################
        # Glue jobs require specific security configurations in order to run succesfully
        # Including an s3 Gateway, Elastic ip, Nat Gateway and route table configurations
        # https://aws.amazon.com/premiumsupport/knowledge-center/glue-s3-endpoint-validation-failed/
        rds_vpc = ec2.Vpc.from_lookup(self, "VPC",
            vpc_id = RDS_VPC_ID,
        )

        rds_vpc.add_gateway_endpoint("S3",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        elastic_ip = ec2.CfnEIP(self, "EIP",
            domain="vpc",
        )

        nat_gateway = ec2.CfnNatGateway(self, "NatGateway",
            subnet_id = RDS_SUBNET_ID,
            allocation_id = elastic_ip.attr_allocation_id,
            connectivity_type="public",
        )
        
        # This is only needed if this route doesn"t already exist
        """cfn_route = ec2.CfnRoute(self, "MyCfnRoute",
            route_table_id=ROUTE_TABLE_ID,
            nat_gateway_id=nat_gateway.attr_nat_gateway_id,
            destination_cidr_block="0.0.0.0/0",
        )"""
        #################################################################################

        # We can now create the JDBC Configuration to Postgres/RDS
        rds_connection = glue.CfnConnection(self, "JDBCConnectionToRDS",
            catalog_id=self.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="JDBCConnectionToRDS",
                connection_type="JDBC",
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=RDS_SUBNET_ID,
                    security_group_id_list=RDS_SECURITY_GROUP_IDS,
                    availability_zone=AVAILABILITY_ZONE
                ),
                connection_properties={
                    "JDBC_CONNECTION_URL": JDBC_CONNECTION_STRING,
                    "USERNAME": RDS_USERNAME,
                    "PASSWORD": RDS_PASSWORD,
                    "JDBC_ENFORCE_SSL": "false"
                },
            ),
        )

        # Create the Glue job with the deployed script and the JDBC connection
        extract_data_job = glue.CfnJob(self, "ExtractJob",
            name="rds_extract_job",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location="s3://big-data-pipeline/scripts/RDSExtract.py"
            ),
            role=glue_role.role_arn,
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=["JDBCConnectionToRDS"]
            ),
            description="Extracts Data from RDS to S3",
            glue_version="3.0",
            worker_type="G.1X",
            number_of_workers=10
        )
