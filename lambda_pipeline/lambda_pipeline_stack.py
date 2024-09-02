from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    Duration,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct
from decouple import config

class LambdaPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Environment settings
        bucket_name = config("BUCKET_NAME")
        api_key = config("API_KEY")
        environment = {"API_KEY": api_key, "BUCKET_NAME": bucket_name}
        
        # Define IAM role for Lambda functions
        lambda_role = self.create_lambda_role()

        # Define Lambda layers
        pandas_layer = self.create_lambda_layer("PandasLayer", "layers/pandaslayer")
        alpha_vantage_layer = self.create_lambda_layer("AlphaVantageLayer", "layers/alphavantage")

        # Define Lambda functions
        convert_historical_data_handler = self.create_lambda_function(
            "ConvertHistoricalDataHandler",
            "convertHistoricalData.handler",
            [pandas_layer],
            environment,
            lambda_role
        )
        intraday_data_handler = self.create_lambda_function(
            "IntradayDataHandler",
            "getIntradayStockData.handler",
            [pandas_layer, alpha_vantage_layer],
            environment,
            lambda_role
        )
        forex_data_handler = self.create_lambda_function(
            "ForexDataHandler",
            "getForexHourlyData.handler",
            [alpha_vantage_layer],
            environment,
            lambda_role
        )

        # S3 bucket configuration and trigger setup
        self.configure_s3_bucket(bucket_name, convert_historical_data_handler)

        # Schedule Lambdas for ticker updates
        self.schedule_lambdas(intraday_data_handler, forex_data_handler)

    def create_lambda_role(self):
        role = iam.Role(self, "LambdaRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"))
        return role

    def create_lambda_layer(self, id, asset_path):
        return lambda_.LayerVersion(self, id, code=lambda_.AssetCode(asset_path))

    def create_lambda_function(self, id, handler, layers, environment, role):
        return lambda_.Function(
            self, id,
            function_name=id,
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda"),
            handler=handler,
            timeout=Duration.seconds(60),  # Default timeout for all functions
            layers=layers,
            environment=environment,
            role=role
        )

    def configure_s3_bucket(self, bucket_name, data_handler):
        bucket = s3.Bucket.from_bucket_name(self, "Bucket", bucket_name)
        notification = s3n.LambdaDestination(data_handler)
        notification.bind(self, bucket)
        bucket.add_object_created_notification(
            notification, s3.NotificationKeyFilter(prefix="data/forex_historical", suffix=".json.gz")
        )

    def schedule_lambdas(self, intraday_data_handler, forex_data_handler):
        tickers = ["MSFT", "AMZN", "IBM"]
        conversions = [
            ("BTC", "CNY"), ("USD", "JPY"), ("USD", "CNY"), ("BTC", "USD")
        ]

        # Schedule stock data updates
        for ticker in tickers:
            rule = events.Rule(self, f"CronRule-{ticker}",
                               schedule=events.Schedule.cron(hour="0", minute="0"))
            rule.add_target(targets.LambdaFunction(intraday_data_handler, event=events.RuleTargetInput.from_object({"ticker": ticker})))

        # Schedule forex data updates
        for from_currency, to_currency in conversions:
            rule = events.Rule(self, f"CronRuleForex-{from_currency}-{to_currency}",
                               schedule=events.Schedule.cron(hour="1,9-23", minute="5"))
            rule.add_target(targets.LambdaFunction(forex_data_handler, event=events.RuleTargetInput.from_object({"from_currency": from_currency, "to_currency": to_currency})))

