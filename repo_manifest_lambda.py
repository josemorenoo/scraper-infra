import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json
from time import time
from typing import Dict

from batch_scraper.manifest.manifest_manager import ManifestManager


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

    secrets: Dict[str, str] = get_secrets()

    # update local repo_manifest
    mm = ManifestManager(secrets)

    manifest_start = time.time()
    local_manifest_path = mm.update_repo_metadata()
    manifest_end = time.time()
    print(f"manifest generation time: {round(manifest_end-manifest_start, 2)}")

    # upload to S3
    s3_client = boto3.resource("s3")
    s3_client.Bucket("coincommit").upload_file(
        local_manifest_path, "assets/repo_manifest.json"
    )
    s3_client.Bucket("coincommit").upload_file(
        local_manifest_path,
        f'assets/repo_manifest_{datetime.now().strftime("%Y-%m-%d")}.json',
    )

    lambda_response = {"status": "done"}

    return lambda_response
