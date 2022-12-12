import json
import boto3
from datetime import datetime, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from botocore.exceptions import ClientError
from botocore.config import Config

from batch_scraper.orchestrator import Orchestrator

boto_config = Config(
    retries={"max_attempts": 0},
    read_timeout=900,
    connect_timeout=900,
    region_name="us-west-1",
)
client = boto3.client("lambda", config=boto_config)


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


def master_lambda_handler(event, context):
    if "start_date" in event and "end_date" in event:
        # pass in start and end to orchestrator for backfilling
        start_date = datetime.strptime(event["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(event["end_date"], "%Y-%m-%d")
        print(
            f'Dates Passed In: orchestrating for {start_date.strftime("%Y-%m-%d")} thru {end_date.strftime("%Y-%m-%d")}'
        )
    else:
        # just use today's date
        end_date = datetime.now()
        start_date = end_date - timedelta(1)

        print(
            f'Today"s date inferred: orchestrating for {start_date.strftime("%Y-%m-%d")} thru {end_date.strftime("%Y-%m-%d")}'
        )

    secrets: Dict[str, str] = get_secrets()
    orch = Orchestrator(start_date=start_date, end_date=end_date, sts_secrets=secrets)
    urls, group_sizes = orch.group_repos()
    repo_url_groups: List[List[str]] = urls
    repo_group_sizes: List[float] = group_sizes
    print(f"repo_url_groups: {len(repo_url_groups)}")

    for i, (repo_group, group_size) in enumerate(
        zip(repo_url_groups, repo_group_sizes)
    ):
        print(
            f"\nsending {len(repo_group)} repos to worker {i}, {round(group_size,2)}mb"
        )

    def invoke_worker(repo_group):
        params = params = {
            "repos_responsible_for": repo_group,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }
        response = client.invoke(
            FunctionName="arn:aws:lambda:us-west-1:665809458133:function:repo-scraper",
            InvocationType="RequestResponse",
            Payload=json.dumps(params),
        )
        return response["Payload"].read()

    """
    results = []
    with ThreadPoolExecutor(max_workers=len(repo_url_groups)) as executor:
        for response in executor.map(invoke_worker, repo_url_groups):
            results.append(response)
    print(results)
    """

    print("done orchestrating")
