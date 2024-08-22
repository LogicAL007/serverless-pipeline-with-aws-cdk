from aws_cdk import (
    Stack,
    aws_glue as glue,
    CfnTag,
    aws_iam as iam,
    aws_events as events,
)
from constructs import Construct
from decouple import config

BUCKET_NAME = config("BUCKET_NAME")
DATABASE_NAME = config("DATABASE_NAME")
TAGS = [
    CfnTag(key="ProjectOwner",value="Alex-Clark"),
    CfnTag(key="ProjectName",value="Big-Data-Pipeline")
]

class GlueDatabaseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_role = iam.Role(self, "GlueDatabaseRole", assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"))

        # Create the glue database
        glue_database = glue.CfnDatabase(self, "FinanceDB",
            catalog_id=self.account, 
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                description="Database with Finance Data",
                location_uri=f"s3://{BUCKET_NAME}/datalake/",
                name=DATABASE_NAME,
            )
        )

        # Create the historical stock table
        historical_stock_table = glue.CfnTable(self, "HistoricalStockTable",
            catalog_id=self.account,
            database_name=DATABASE_NAME,
            table_input=glue.CfnTable.TableInputProperty(
                description="Historical Stock Data",
                name="historical_stock_data",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="ticker", type="string"),
                        glue.CfnTable.ColumnProperty(name="date", type="date"),
                        glue.CfnTable.ColumnProperty(name="open", type="double"),
                        glue.CfnTable.ColumnProperty(name="high", type="double"),
                        glue.CfnTable.ColumnProperty(name="low", type="double"),
                        glue.CfnTable.ColumnProperty(name="close", type="double"),
                        glue.CfnTable.ColumnProperty(name="adj_close", type="double"),
                        glue.CfnTable.ColumnProperty(name="volume", type="double")
                    ],
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    location="s3://big-data-pipeline/datalake/stock_data_historical/",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                )
            )
        )

        # Create the historical forex table
        historical_forex_data = glue.CfnTable(self, "HistoricalForexData",
            catalog_id=self.account,
            database_name=DATABASE_NAME,
            table_input=glue.CfnTable.TableInputProperty(
                description="Historical Forex Data",
                name="forex_daily_historical",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="from_currency", type="string"),
                        glue.CfnTable.ColumnProperty(name="to_currency", type="string"),
                        glue.CfnTable.ColumnProperty(name="date", type="date"),
                        glue.CfnTable.ColumnProperty(name="open", type="double"),
                        glue.CfnTable.ColumnProperty(name="high", type="double"),
                        glue.CfnTable.ColumnProperty(name="low", type="double"),
                        glue.CfnTable.ColumnProperty(name="close", type="double"),
                        glue.CfnTable.ColumnProperty(name="adj_close", type="double"),
                        glue.CfnTable.ColumnProperty(name="volume", type="double")
                    ],
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        location="s3://big-data-pipeline/datalake/forex_historical/",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                )
            )
        ) 

        #A crawler to crawl the intraday data
        intraday_stock_data_crawler = glue.CfnCrawler(self, "IntradayStockDataCrawler",
            role=glue_role.role_name,
            name="intraday_stock_data_crawler",
            database_name=DATABASE_NAME,
            schedule=events.Schedule.cron(hour="2",minute="0"),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{BUCKET_NAME}/datalake/stock_data_intraday/",
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
            ),
        )

        #A crawler for the forex hourly data
        forex_hourly_crawler = glue.CfnCrawler(self, "ForexHourlyCrawler",
            role=glue_role.role_name,
            name="forex_hourly_crawler",
            database_name=DATABASE_NAME,
            schedule=events.Schedule.cron(hour="2",minute="0"),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{BUCKET_NAME}/datalake/forex_hourly/",
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
            ),
        )

        # Make the tables and crawlers dependent on the database creation
        historical_stock_table.add_depends_on(glue_database)
        historical_forex_data.add_depends_on(glue_database)
        intraday_stock_data_crawler.add_depends_on(glue_database)
        forex_hourly_crawler.add_depends_on(glue_database)