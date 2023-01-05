import json
import boto3
from datetime import datetime, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from collections import defaultdict
import os
import shutil
import time

from botocore.exceptions import ClientError
from botocore.config import Config

from batch_scraper.orchestrator import Orchestrator
from batch_scraper.manifest.manifest_manager import ManifestManager

boto_config = Config(
    retries={"max_attempts": 0},
    read_timeout=900,
    connect_timeout=900,
    region_name="us-west-1",
    max_pool_connections=50,
)
lambda_client = boto3.client("lambda", config=boto_config)
s3_client = boto3.resource("s3")


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


def join_reports(results: str, report_date: str):
    # clean working directory
    assembly_dir = "/tmp/report_pieces/"
    if os.path.exists(assembly_dir):
        shutil.rmtree(assembly_dir)
    if not os.path.exists(assembly_dir):
        os.makedirs(assembly_dir, exist_ok=True)

    master_report = {}
    durations = []
    for worker_result in results:
        decoded_result = json.loads(worker_result.decode("utf-8"))

        # tally durations for performance tuning
        duration_sec = round(int(decoded_result["worker_duration_secs"]), 3)
        worker_id = decoded_result["worker_id"]
        durations.append((worker_id, duration_sec))

        # download and extract report
        report_path_pieces = decoded_result["report_path"].split("/")
        bucket = report_path_pieces[2]
        report_path = "/".join(report_path_pieces[3:])
        local_report_path = assembly_dir + report_path_pieces[-1]
        s3_client.Bucket(bucket).download_file(report_path, local_report_path)

        with open(local_report_path, "r") as f:
            worker_report = json.load(f)
            for token_name, token_data in worker_report.items():
                if token_name in master_report:
                    master_report[token_name]["commit_count"] += token_data[
                        "commit_count"
                    ]
                    master_report[token_name]["lines_of_code"] += token_data[
                        "lines_of_code"
                    ]
                    master_report[token_name]["commit_messages"] += token_data[
                        "commit_messages"
                    ]

                    master_report[token_name]["distinct_authors"] += token_data[
                        "distinct_authors"
                    ]

                    master_report[token_name]["commit_urls"] += token_data[
                        "commit_urls"
                    ]

                    master_report[token_name]["changed_methods"] += token_data[
                        "changed_methods"
                    ]

                    for fext_name, fext_count in token_data["file_extensions"].items():
                        if fext_name in master_report[token_name]["file_extensions"]:
                            master_report[token_name]["file_extensions"][
                                fext_name
                            ] += fext_count
                        else:
                            master_report[token_name]["file_extensions"][
                                fext_name
                            ] = fext_count

                    for fext_action, fext_data in token_data[
                        "loc_changes_by_filetype"
                    ].items():
                        for fext_name, fext_count in fext_data.items():
                            if (
                                fext_action
                                not in master_report[token_name][
                                    "loc_changes_by_filetype"
                                ]
                            ):
                                master_report[token_name]["loc_changes_by_filetype"][
                                    fext_action
                                ] = token_data["loc_changes_by_filetype"][fext_action]
                            else:
                                if (
                                    fext_name
                                    in master_report[token_name][
                                        "loc_changes_by_filetype"
                                    ][fext_action]
                                ):
                                    master_report[token_name][
                                        "loc_changes_by_filetype"
                                    ][fext_action][fext_name] += fext_count
                                else:
                                    master_report[token_name][
                                        "loc_changes_by_filetype"
                                    ][fext_action][fext_name] = fext_count
                else:
                    master_report[token_name] = {}
                    master_report[token_name]["commit_count"] = token_data[
                        "commit_count"
                    ]
                    master_report[token_name]["lines_of_code"] = token_data[
                        "lines_of_code"
                    ]
                    master_report[token_name]["commit_messages"] = token_data[
                        "commit_messages"
                    ]

                    master_report[token_name]["distinct_authors"] = token_data[
                        "distinct_authors"
                    ]

                    master_report[token_name]["commit_urls"] = token_data["commit_urls"]

                    master_report[token_name]["changed_methods"] = token_data[
                        "changed_methods"
                    ]

                    master_report[token_name]["file_extensions"] = token_data[
                        "file_extensions"
                    ]

                    master_report[token_name]["loc_changes_by_filetype"] = token_data[
                        "loc_changes_by_filetype"
                    ]

    master_report_local_path = f"/tmp/{report_date}.json"
    with open(master_report_local_path, "w") as fp:
        json.dump(master_report, fp, indent=2)
        print(f"dumped report to {master_report_local_path}")

    object_name = f"reports/{report_date}/{report_date}.json"
    s3_client.Bucket("coincommit").upload_file(master_report_local_path, object_name)
    print("durations: ", durations)


def dump_empty_report(report_date):
    master_report = {}
    master_report_local_path = f"/tmp/{report_date}.json"
    with open(master_report_local_path, "w") as fp:
        json.dump(master_report, fp, indent=2)
        print(f"dumped EMPTY report to {master_report_local_path}")

    object_name = f"reports/{report_date}/{report_date}.json"
    s3_client.Bucket("coincommit").upload_file(master_report_local_path, object_name)


def master_lambda_handler(event, context):
    secrets: Dict[str, str] = get_secrets()

    start = time.time()
    mm = ManifestManager(secrets)
    mm.update_repo_metadata()
    end = time.time()
    print(f"Took {round(end-start, 2)} to update repo manifest")

    start = time.time()
    if "start_date" in event and "end_date" in event:
        # pass in start and end to orchestrator for backfilling
        start_date = datetime.strptime(event["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(event["end_date"], "%Y-%m-%d")
        print(
            f'Dates Passed In: orchestrating for {start_date.strftime("%Y-%m-%d")} thru {end_date.strftime("%Y-%m-%d")}'
        )
    else:
        # just use today's date
        end_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
        start_date = end_date - timedelta(1)
        start_date = datetime.strptime(start_date.strftime("%Y-%m-%d"), "%Y-%m-%d")

        print(
            f'Today"s date inferred: orchestrating for {start_date.strftime("%Y-%m-%d")} thru {end_date.strftime("%Y-%m-%d")}'
        )

    if "backfilling" in event:
        backfilling = bool(event["backfilling"])
    else:
        backfilling = False

    orch = Orchestrator(
        start_date=start_date,
        end_date=end_date,
        sts_secrets=secrets,
        backfilling=backfilling,
    )
    urls, group_sizes = orch.group_repos()
    repo_url_groups: List[List[str]] = urls
    repo_group_sizes: List[float] = group_sizes
    print(f"repo_url_groups: {len(repo_url_groups)}")

    for i, (repo_group, group_size) in enumerate(
        zip(repo_url_groups, repo_group_sizes)
    ):
        print(
            f"\nsending {len(repo_group)} repos to worker {i}, max potential size: {round(group_size,2)}mb"
        )

    def invoke_worker(repo_group, worker_id):
        params = {
            "repos_responsible_for": repo_group,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "worker_id": worker_id,
        }
        response = lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-west-1:665809458133:function:repo-scraper",
            InvocationType="RequestResponse",
            Payload=json.dumps(params),
        )
        return response["Payload"].read()

    results = []
    if len(repo_url_groups) == 0:
        print(f"Devs are sleeping, no commits anywhere between {start_date} {end_date}")
    else:
        with ThreadPoolExecutor(max_workers=len(repo_url_groups)) as executor:
            futures = [
                executor.submit(invoke_worker, repo_group, i)
                for i, repo_group in enumerate(repo_url_groups)
            ]
            for future in as_completed(futures):
                worker_result = future.result()
                results.append(worker_result)
                print(worker_result)

    """
    with ThreadPoolExecutor(max_workers=len(repo_url_groups)) as executor:
        for response in executor.map(
            invoke_worker, repo_url_groups, range(len(repo_url_groups))
        ):
            results.append(response)
    print(*results, sep="\n")
    """

    # only join valid results
    valid_results = [
        r for r in results if "report_path" in json.loads(r.decode("utf-8"))
    ]
    invalid_results = [
        r for r in results if "report_path" not in json.loads(r.decode("utf-8"))
    ]

    print(f"{len(results)} workers, valid results {len(valid_results)}")

    # join worker reports
    if len(valid_results) > 0:
        join_reports(valid_results, report_date=end_date)
    else:
        dump_empty_report(report_date=end_date)

    end = time.time()
    lambda_results = {
        "results_count": len(results),
        "valid_results_count": len(valid_results),
        "invalid_results_count": len(invalid_results),
        "invalid_results": invalid_results,
        "duration": round(end - start, 3),
        "report": f'{end_date.strftime("%Y-%m-%d")}.json',
    }

    print("done orchestrating")
    return lambda_results
