import aws_cdk as core
import aws_cdk.assertions as assertions

from serverless_error_retry.serverless_error_retry_stack import ServerlessErrorRetryStack

# example tests. To run these tests, uncomment this file along with the example
# resource in serverless_error_retry/serverless_error_retry_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ServerlessErrorRetryStack(app, "serverless-error-retry")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
