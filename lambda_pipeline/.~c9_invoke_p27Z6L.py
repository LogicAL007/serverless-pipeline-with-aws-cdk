from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    Duration,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct
from decouple import config

BUCKET_NAME = config('BUCKET_NAME')
LAMBDA_RUNTIME=_lambda.Runtime.PYTHON_3_7
TICKERS = ['MSFT', 'AMZN', 'IBM']

CONVERSIONS = [
    ('BTC', 'CNY'),
    ('USD', 'JPY'),
    ('USD', 'CNY'),
    ('BTC', 'USD')
]

ENVIRONMENT={
    'API_KEY': config('API_KEY'),
    'BUCKET_NAME': BUCKET_NAME
}

class LambdaPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_role = iam.Role(self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'))
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess'))

        # Create a layer with the pandas package
        pandasLayer = _lambda.LayerVersion(
            self,
            'pandasLayer',
            code = _lambda.AssetCode('layers/pandasLayer'),
         )

        # Create a layer with the alpha_vantage package
        alphaVantageLayer = _lambda.LayerVersion(
            self,
            'alphaVantageLayer',
            code = _lambda.AssetCode('layers/alphaVantageLayer'),
         )

        # Create lambda to perform ETL on historical data
        convert_historical_data_handler = _lambda.Function(self, 'ConvertHistoricalDataHandler',
            function_name="historical_data_handler",
            runtime=LAMBDA_RUNTIME,
            code=_lambda.Code.from_asset('lambda'),
            timeout=Duration.seconds(30),
            layers=[pandasLayer],
            handler='convertHistoricalData.handler',
            environment=ENVIRONMENT,
            role=lambda_role,
        )

        # Create lambda to perform ETL on intraday stock data
        intraday_data_handler = _lambda.Function(self, 'IntradayDataHandler',
            function_name="intraday_data_handler",
            runtime=LAMBDA_RUNTIME,
            code=_lambda.Code.from_asset('lambda'),
            timeout=Duration.seconds(60),
            layers=[pandasLayer, alphaVantageLayer],
            handler='getIntradayStockData.handler',
            environment=ENVIRONMENT,
            role=lambda_role,
        )

        # Lambda to retrieve hourly forex data
        forex_data_handler = _lambda.Function(self, 'ForexDataHandler',
            function_name="hourly_forex_handler",
            runtime=LAMBDA_RUNTIME,
            code=_lambda.Code.from_asset('lambda'),
            timeout=Duration.seconds(30),
            layers=[alphaVantageLayer],
            handler='getForexHourlyData.handler',
            environment=ENVIRONMENT,
            role=lambda_role,
        )

        ########################################################################################
        #Add Trigger to run historical lambda when new files are added
        bucket = s3.Bucket.from_bucket_name(self, 'bucket', BUCKET_NAME)
        notification = s3n.LambdaDestination(convert_historical_data_handler)
        notification.bind(self, bucket)

        bucket.add_object_created_notification(
            notification,
            s3.NotificationKeyFilter(prefix='data/forex-historical',suffix='.json.gz')
        )
        ########################################################################################


        ########################################################################################
        # Schedule the intrday handler for the tickers listed in the TICKERS list
        for ticker in TICKERS:
            rule = events.Rule(self, f'CronRule-{ticker}',
                schedule=events.Schedule.cron(hour="0",minute="0")
            )

            target = targets.LambdaFunction(
                intraday_data_handler,
                event=events.RuleTargetInput.from_object({"ticker": ticker})
            )

            rule.add_target(target)
        ########################################################################################


        ########################################################################################
        # Schedule the hourly forex data for the conversions in the CONVERSION list
        for conversion in CONVERSIONS:
            rule = events.Rule(self, f'CronRuleForex-{conversion[0]}-{conversion[1]}',
                schedule=events.Schedule.cron(hour='1,9-23', minute="5")
            )

            target = targets.LambdaFunction(
                forex_data_handler,
                event=events.RuleTargetInput.from_object(
                    {
                        "from_currency": conversion[0],
                        "to_currency": conversion[1]
                    }
                )
            )
            rule.add_target(target)
        ########################################################################################