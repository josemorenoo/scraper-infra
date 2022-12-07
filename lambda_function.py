from batch_scraper.run import Runner

import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json
import os
from typing import Dict


def get_secrets() -> Dict[str, str]:
    secret_name = "reposcraper-keys"
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
    print(event)
    repos_for_processing = event["body"]["repos_responsible_for"]
    if len(repos_for_processing) == 0:
        return f"repos_for_processing is empty!"
    else:
        print(
            f"repos_for_processing contains {len(repos_for_processing)} items: {repos_for_processing}"
        )

    secrets: Dict[str, str] = get_secrets()

    runner = Runner(
        job_hash=datetime.today().strftime("%Y-%m-%d"),
        worker_id=context.aws_request_id,
        repos_responsible_for=repos_for_processing,
        sts_secrets=secrets,
    )
    s3_generated_report_path = runner.run()

    # cleanup
    os.system("rm -rf /tmp/*")

    s3_path = f"s3://coincommit/{s3_generated_report_path}"
    print(f"output: {s3_path}")

    lambda_response = {
        "report_path": s3_path,
        "invocation_id": context.aws_request_id,
        "responsible_for": ",".join(repos_for_processing),
    }

    return lambda_response
