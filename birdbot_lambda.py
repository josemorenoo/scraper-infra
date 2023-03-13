from birdbot.birdbot import BirdBot

import boto3
from botocore.exceptions import ClientError
import json
import os
from typing import Dict
import time


def get_secrets() -> Dict[str, str]:
    secret_name = "coincommitsecrets"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        print("STS get_secrets:", e)
        raise e

    # Decrypts secret using the associated KMS key.
    secrets = get_secret_value_response["SecretString"]
    return json.loads(secrets)


def lambda_handler(event, context):
    # event comes in as a dictionary

    sts_secrets: Dict[str, str] = get_secrets()

    start = time.time()
    bird = BirdBot(sts_secrets)
    success = bird.tweet()

    # cleanup
    os.system("rm -rf /tmp/*")

    end = time.time()
    lambda_response = {"worker_duration_secs": end - start, "success": success}
    return lambda_response
