from aws_cdk import (
    CfnOutput,
    Stack,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_destinations as destinations,
    aws_lambda_event_sources as event_soruces,
    aws_events_targets as events_target,
    aws_events as events,
    aws_iam as iam,
    aws_pipes as pipes
)
from constructs import Construct

class ServerlessErrorRetryStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        SNS_TOPIC_ONE_RETRY_COUNT = 3
        SNS_TOPIC_TWO_RETRY_COUNT = 5
        ERROR_TYPES_SHOULD_NOT_RETRY = ['KeyError']

        dl_queue = sqs.Queue(self, 'DlQueue')

        source_sns_topic_one = sns.Topic(self, 'sourceTopicOne')
        source_sns_topic_two = sns.Topic(self, 'sourceTopicTwo')

        sns_retry_exhausted_queue = sqs.Queue(self, 'SnsRetryExhaustedQueue')

        function_one = _lambda.Function(
            self, 'FunctionOne',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambda/function_one'),
            handler='index.handler',
            retry_attempts=0,
            on_failure=destinations.SqsDestination(dl_queue),
        )

        function_one.add_event_source(event_soruces.SnsEventSource(source_sns_topic_one))

        function_two = _lambda.Function(
            self, 'FunctionTwo',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambda/function_two'),
            handler='index.handler',
            retry_attempts=0,
            on_failure=destinations.SqsDestination(dl_queue),
        )

        function_two.add_event_source(event_soruces.SnsEventSource(source_sns_topic_two))

        ##### Target Event Bus
        target_events_bus = events.EventBus(self, 'EventsBus')

        # Add event bus rule and target for sns topic one
        event_rule_sns_topic_one_redrive = events.Rule(self, 'eventRuleForSnsTopicOne',
            event_bus=target_events_bus,
            event_pattern=events.EventPattern(
                source=['myapp'],
                detail_type=['FailedEvent'],
                detail={
                    'meta': {   
                        'source_topic': [source_sns_topic_one.topic_arn],
                        'retry_count': [{"numeric":["<", SNS_TOPIC_ONE_RETRY_COUNT]}],
                        'error_type': [{"anything-but": ERROR_TYPES_SHOULD_NOT_RETRY}]
                    }
                },
            ),
        );

        event_rule_sns_topic_one_redrive.add_target(events_target.SnsTopic(source_sns_topic_one, message=events.RuleTargetInput.from_event_path('$.detail.payload')))

        # Add event bus rule and target for sns topic two
        event_rule_sns_topic_two_redrive = events.Rule(self, 'eventRuleForSnsTopicTwo',
            event_bus=target_events_bus,
            event_pattern=events.EventPattern(
                source=['myapp'],
                detail_type=['FailedEvent'],
                detail={
                    'meta': {   
                        'source_topic': [source_sns_topic_two.topic_arn],
                        'retry_count': [{"numeric":["<", SNS_TOPIC_TWO_RETRY_COUNT]}],
                        'error_type': [{"anything-but": ERROR_TYPES_SHOULD_NOT_RETRY}]
                    }
                },
            ),
        );

        event_rule_sns_topic_two_redrive.add_target(events_target.SnsTopic(source_sns_topic_two, message=events.RuleTargetInput.from_event_path('$.detail.payload')))

        # Add event bus rule for exhausted retries
        event_rule_exhausted_retries = events.Rule(self, 'eventRuleForExhaustedRetries',
            event_bus=target_events_bus,
            event_pattern=events.EventPattern(
                source=['myapp'],
                detail_type=['FailedEvent'],
                detail={
                    'meta': {
                        '$or': [
                            {
                                'source_topic': [source_sns_topic_one.topic_arn],
                                'retry_count': [{"numeric":[">=", SNS_TOPIC_ONE_RETRY_COUNT]}],
                                'error_type': [{"anything-but": ERROR_TYPES_SHOULD_NOT_RETRY}]
                            },
                            {
                                'source_topic': [source_sns_topic_two.topic_arn],
                                'retry_count': [{"numeric":[">=", SNS_TOPIC_TWO_RETRY_COUNT]}],
                                'error_type': [{"anything-but": ERROR_TYPES_SHOULD_NOT_RETRY}]
                            }
                        ]
                    }
                },
            ),
        );

        event_rule_exhausted_retries.add_target(events_target.SqsQueue(sns_retry_exhausted_queue, message=events.RuleTargetInput.from_event_path('$.detail')))

        ##### EventBridge Pipe with Enrichment
        pipe_source_policy = iam.PolicyStatement(
                actions=['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
                resources=[dl_queue.queue_arn],
                effect=iam.Effect.ALLOW,
        )

        pipe_target_policy = iam.PolicyStatement(
                actions=['events:PutEvents'],
                resources=[target_events_bus.event_bus_arn],
                effect=iam.Effect.ALLOW,
        )

        enrichment_lambda = _lambda.Function(
            self, 'EnrichmentLambdaFunction',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambda/enrichment'),
            handler='index.handler',
            retry_attempts=0,
        )

        pipe_enrichment_policy = iam.PolicyStatement(
                actions=['lambda:InvokeFunction'],
                resources=[enrichment_lambda.function_arn],
                effect=iam.Effect.ALLOW,
        )

        ### Pipe role
        pipe_role = iam.Role(self, 'PipePolicy',
            assumed_by=iam.ServicePrincipal('pipes.amazonaws.com'),
        )

        pipe_role.add_to_policy(pipe_source_policy)
        pipe_role.add_to_policy(pipe_target_policy)
        pipe_role.add_to_policy(pipe_enrichment_policy)

        ### Pipe
        pipe = pipes.CfnPipe(self, "FailedEventRetryPipe",
            role_arn=pipe_role.role_arn,
            source=dl_queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    # batch_size=5,
                    # maximum_batching_window_in_seconds=10
                )
            ),
            enrichment=enrichment_lambda.function_arn,
            target=target_events_bus.event_bus_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                event_bridge_event_bus_parameters=pipes.CfnPipe.PipeTargetEventBridgeEventBusParametersProperty(
                    detail_type="FailedEvent",
                    source="myapp",
                ),
            )
        )
        
        # Output
        CfnOutput(self, "DeadLetterQueue", value=dl_queue.queue_arn)
        CfnOutput(self, "SourceSNSTopicOne", value=source_sns_topic_one.topic_arn)
        CfnOutput(self, "SourceSNSTopicTwo", value=source_sns_topic_two.topic_arn)
        CfnOutput(self, "PIPE_ARN", value=pipe.attr_arn)
        CfnOutput(self, "RetryExaustedQueue", value=sns_retry_exhausted_queue.queue_arn)
