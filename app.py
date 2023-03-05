#!/usr/bin/env python3
import os

import aws_cdk as cdk

from serverless_error_retry.serverless_error_retry_stack import ServerlessErrorRetryStack


app = cdk.App()
ServerlessErrorRetryStack(app, "ServerlessErrorRetryStack")

app.synth()
