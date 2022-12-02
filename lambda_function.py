from batch_scraper.run import Runner

import boto3
from botocore.exceptions import ClientError
import json
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
    secrets: Dict[str, str] = get_secrets()

    runner = Runner(
        job_hash="xxxxx",
        worker_id=context.aws_request_id,
        tokens_responsible_for=["ETH", "BTC"],
        sts_secrets=secrets,
    )
    s3_generated_report_path = runner.run()
    s3_path = f"s3://coincommit/{s3_generated_report_path}"
    print(f"output: {s3_path}")
    """
        scraper = ScrapeCMC(secrets)
        description = scraper.scrape_project_description("ethereum")
        if description:
            print(description)
        else:
            print("got back no description, huh")
    """

    return s3_path
