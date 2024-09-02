import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


#Create dynamic frame using JDBC Connection
def directJDBCSource(
        glueContext,
        connectionName,
        connectionType,
        database,
        table,
        redshiftTmpDir,
        transformation_ctx,
    ) -> DynamicFrame:

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Connect to RDS Database
PostgreSQLtable_node1 = directJDBCSource(
    glueContext,
    connectionName="JDBCConnectionToRDS",
    connectionType="postgresql",
    database="postgres",
    table="financedb.stock_data_historical",
    redshiftTmpDir="",
    transformation_ctx="PostgreSQLtable_node1",
)

# Write data from RDS to S3
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=PostgreSQLtable_node1,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://big-data-pipeline/datalake/stock_data_historical/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
