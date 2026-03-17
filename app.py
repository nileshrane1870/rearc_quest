#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_cdk import (
    aws_lambda as _lambda,
    aws_lambda_event_sources,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)

class DataPipelineStack(cdk.Stack):

    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket to store the data
        bucket = s3.Bucket(self, "RearcQuestBucket",
                           removal_policy=cdk.RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        # SQS Queue to receive notifications
        queue = sqs.Queue(self, "RearcQuestQueue")

        # IAM Role that the Lambda functions will assume
        lambda_role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"),
            ]
        )

        # Part 1 & 2 Lambda: Fetches data and puts it in S3
        data_fetcher_lambda = _lambda.PythonFunction(
            self, "DataFetcher",
            runtime=_lambda.Runtime.PYTHON_3_9,
            entry="lambda",  # Directory with the lambda code
            index="lambda_handler.py",  # File with the handler
            handler="handler",  # Function name
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name
            }
        )

        # Schedule the data fetcher to run once a day
        rule = events.Rule(
            self, "Rule",
            schedule=events.Schedule.cron(minute="0", hour="0"),
        )
        rule.add_target(targets.LambdaFunction(data_fetcher_lambda))

        # Part 3 Lambda: Runs analysis on the data in S3
        analysis_lambda = _lambda.PythonFunction(
            self, "AnalysisRunner",
            runtime=_lambda.Runtime.PYTHON_3_9,
            entry="lambda",
            index="analysis_handler.py",
            handler="handler",
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name
            }
        )

        # Trigger the SQS queue when the population JSON file is added to S3
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="population_data.json")
        )
        
        # Trigger the analysis lambda from the SQS queue
        analysis_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(queue)
        )

        # Output the bucket name for easy access
        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)


app = cdk.App()
DataPipelineStack(app, "RearcDataQuestStack")
app.synth()
