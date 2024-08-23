import aws_cdk as core
import aws_cdk.assertions as assertions

from big_data_pipeline.big_data_pipeline_stack import BigDataPipelineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in big_data_pipeline/big_data_pipeline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = BigDataPipelineStack(app, "big-data-pipeline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
