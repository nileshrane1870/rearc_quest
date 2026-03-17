#!/usr/bin/env python3
import os

import aws_cdk as cdk
from cdk_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "RearcDataPipelineStack",
    # It's a best practice to explicitly define the environment for your stacks
    # to ensure your app is deployable to any region/account.
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)
app.synth()
