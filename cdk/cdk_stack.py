from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as lambda_event_sources,
    Duration,
    RemovalPolicy,
)
from constructs import Construct


class DataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. S3 Bucket to store all data
        # This bucket will be automatically deleted when the stack is destroyed.
        data_bucket = s3.Bucket(
            self, "RearcQuestDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # 2. SQS Queue for S3 notifications
        # Messages will be invisible for 5 minutes after being read by the lambda,
        # giving it enough time to process the analysis.
        analysis_queue = sqs.Queue(
            self, "AnalysisQueue",
            visibility_timeout=Duration.minutes(5)
        )

        # 3. S3 notification to send a message to SQS when a .json file is created
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(analysis_queue),
            s3.NotificationKeyFilter(suffix=".json")
        )

        # 4. IAM Role for the Ingestion Lambda
        # This role grants basic execution permissions and S3 read/write access.
        ingestion_role = iam.Role(
            self, "IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        data_bucket.grant_read_write(ingestion_role)

        # 5. Ingestion Lambda Function (Parts 1 & 2)
        # The code is located in the ../lambda/ingestion directory relative to this cdk app.
        ingestion_lambda = _lambda.Function(
            self, "IngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("../lambda/ingestion"),
            role=ingestion_role,
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name
            }
        )

        # 6. EventBridge Rule to run the Ingestion Lambda daily
        events.Rule(
            self, "DailyIngestionRule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(ingestion_lambda)]
        )

        # 7. IAM Role for the Analysis Lambda
        # This role grants basic execution, S3 read, and SQS message consumption permissions.
        analysis_role = iam.Role(
            self, "AnalysisLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        data_bucket.grant_read(analysis_role)
        analysis_queue.grant_consume_messages(analysis_role)

        # 8. Analysis Lambda Function (Part 3)
        # The code is located in the ../lambda/analysis directory.
        # Pandas can be memory-intensive, so we allocate more memory.
        analysis_lambda = _lambda.Function(
            self, "AnalysisLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("../lambda/analysis"),
            role=analysis_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name,
            }
        )

        # 9. Connect the SQS queue to the Analysis Lambda
        analysis_lambda.add_event_source(lambda_event_sources.SqsEventSource(analysis_queue))