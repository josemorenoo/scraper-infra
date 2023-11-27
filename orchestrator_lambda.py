import json
import boto3
from datetime import datetime, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os
import shutil
import time

from botocore.exceptions import ClientError
from botocore.config import Config

from batch_scraper.orchestrator import Orchestrator

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


def _update_master_report(master_report, token_name, token_data):
    if token_name not in master_report:
        master_report[token_name] = token_data
        return

    # Aggregate data for existing token
    for key in ["commit_count", "lines_of_code", "commit_messages", "distinct_authors", "commit_urls", "changed_methods", "active_repos"]:
        master_report[token_name][key] += token_data[key]

    # File extensions
    for fext_name, fext_count in token_data["file_extensions"].items():
        master_report[token_name]["file_extensions"].setdefault(fext_name, 0)
        master_report[token_name]["file_extensions"][fext_name] += fext_count

    # LOC changes by filetype
    for fext_action, fext_data in token_data["loc_changes_by_filetype"].items():
        if fext_action not in master_report[token_name]["loc_changes_by_filetype"]:
            master_report[token_name]["loc_changes_by_filetype"][fext_action] = fext_data
        else:
            for fext_name, fext_count in fext_data.items():
                master_report[token_name]["loc_changes_by_filetype"][fext_action].setdefault(fext_name, 0)
                master_report[token_name]["loc_changes_by_filetype"][fext_action][fext_name] += fext_count

def join_reports(s3_report_paths: List[str], report_date_str: str, report_name: str):
    """
    Given a list of S3 report paths, combine them into a single JSON file.  If a report does not exist in S3, skip it
    """
    print(f"Joining the following reports into {report_name}:")
    print(*s3_report_paths, sep="\n")

    master_report = {}
    assembly_dir = "/tmp/reports_to_join/"
    if not os.path.exists(assembly_dir):
        os.makedirs(assembly_dir, exist_ok=True)

    s3_client = boto3.client('s3')

    for report_location in s3_report_paths:
        report_path_pieces = report_location.split("/")
        bucket = report_path_pieces[2]
        report_path = "/".join(report_path_pieces[3:])
        local_report_path = os.path.join(assembly_dir, report_path_pieces[-1])


        # Check if report exists in S3
        try:
            s3_client.head_object(Bucket=bucket, Key=report_path)
        except ClientError:
            # Skip this report if it does not exist
            print(f"Report not found: {report_location}")
            continue

        # Download and process the report
        s3_client.download_file(bucket, report_path, local_report_path)
        with open(local_report_path, "r") as f:
            worker_report = json.load(f)
            for token_name, token_data in worker_report.items():
                _update_master_report(master_report, token_name, token_data)

    # Upload master report
    master_report_local_path = f"/tmp/{report_name}.json"
    with open(master_report_local_path, "w") as fp:
        json.dump(master_report, fp, indent=2)
        print(f"Dumped report to {master_report_local_path}")

    object_name = f"reports/{report_date_str}/{report_name}.json"
    s3_client.upload_file(master_report_local_path, "coincommit", object_name)
    print(f"Uploaded {master_report_local_path} to {object_name}")


def join_worker_reports(results: str, report_date: str):
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
        durations.append(
            (
                "id_" + str(worker_id),
                str(duration_sec) + " secs",
                "aka " + str(round(float(duration_sec) / 60.0, 2)) + " mins",
            )
        )

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

                    master_report[token_name]["active_repos"] += token_data[
                        "active_repos"
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

                    master_report[token_name]["active_repos"] = token_data[
                        "active_repos"
                    ]

                    # these don't need to be appended, just assigned
                    master_report[token_name]["description"] = token_data["description"]
                    master_report[token_name]["project_created_cmc"] = token_data[
                        "project_created_cmc"
                    ]

    report_date_str = report_date.strftime("%Y-%m-%d")
    master_report_local_path = f"/tmp/{report_date_str}.json"
    with open(master_report_local_path, "w") as fp:
        json.dump(master_report, fp, indent=2)
        print(f"dumped report to {master_report_local_path}")

    object_name = f"reports/{report_date_str}/{report_date_str}.json"
    s3_client.Bucket("coincommit").upload_file(master_report_local_path, object_name)
    print("durations: ", durations)


def dump_empty_report(report_date: datetime):
    report_date_str = report_date.strftime("%Y-%m-%d")

    master_report = {}
    master_report_local_path = f"/tmp/{report_date_str}.json"
    with open(master_report_local_path, "w") as fp:
        json.dump(master_report, fp, indent=2)
        print(f"dumped EMPTY report to {master_report_local_path}")

    object_name = f"reports/{report_date_str}/{report_date_str}.json"
    s3_client.Bucket("coincommit").upload_file(master_report_local_path, object_name)

def _get_date_range(days: int):
    """
    Generator function to yield dates for the given range.
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    for n in range((end_date - start_date).days + 1):
        yield (start_date + timedelta(n)).strftime("%Y-%m-%d")

def _generate_windowed_report(range_days, report_name):
    today_str = datetime.now().date().strftime("%Y-%m-%d")
    report_range = _get_date_range(days=range_days)
    s3_report_paths = [
        f"s3://coincommit/reports/{report_date_str}/{report_date_str}.json"
        for report_date_str in report_range
    ]
    join_reports(
        s3_report_paths, report_date_str=today_str, report_name=report_name
    )

def master_lambda_handler(event, context):
    secrets: Dict[str, str] = get_secrets()

    if "backfilling" in event:
        backfilling = bool(event["backfilling"])
    else:
        backfilling = False

    start = time.time()
    if "start_date" in event and "end_date" in event:
        # pass in start and end to orchestrator for backfilling
        start_date: datetime = datetime.strptime(event["start_date"], "%Y-%m-%d")
        end_date: datetime = datetime.strptime(event["end_date"], "%Y-%m-%d")
        print(
            f'Dates Passed In: orchestrating for {start_date.strftime("%Y-%m-%d")} thru {end_date.strftime("%Y-%m-%d")}'
        )
    else:
        # just use today's date
        end_date: datetime = datetime.strptime(
            datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d"
        )
        start_date = end_date - timedelta(1)
        start_date: datetime = datetime.strptime(
            start_date.strftime("%Y-%m-%d"), "%Y-%m-%d"
        )

        print(
            f"Today's date inferred: orchestrating for {start_date.strftime('%Y-%m-%d')} thru {end_date.strftime('%Y-%m-%d')}"
        )

    orch = Orchestrator(
        start_date=start_date,
        end_date=end_date,
        sts_secrets=secrets,
        lambda_mem_limit_mb=800.0,
        backfilling=backfilling,
        download_manifest_from_s3=True,
    )
    urls, group_sizes = orch.group_repos()
    repo_url_groups: List[List[str]] = urls
    repo_group_sizes: List[float] = group_sizes
    print(
        f"calculated repo groupings for workers, seconds remaining: {context.get_remaining_time_in_millis()/1000.0}"
    )
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
                print(
                    f"response came back from worker, seconds remaining: {context.get_remaining_time_in_millis() / 1000.0}"
                )
                print(worker_result)

    """
    with ThreadPoolExecutor(max_workers=len(repo_url_groups)) as executor:
        for response in executor.map(
            invoke_worker, repo_url_groups, range(len(repo_url_groups))
        ):
            results.append(response)
    print(*results, sep="\n")
    """

    print(
        f"joining valid resuts, seconds remaining: {context.get_remaining_time_in_millis() / 1000.0}"
    )

    # only join valid results
    valid_results = [
        r for r in results if "report_path" in json.loads(r.decode("utf-8"))
    ]
    invalid_results = [
        r for r in results if "report_path" not in json.loads(r.decode("utf-8"))
    ]

    print(f"{len(results)} workers, valid results {len(valid_results)}")

    if len(valid_results) > 0:
        # join worker reports
        join_worker_reports(valid_results, report_date=end_date)

        # make weekly raw report
        _generate_windowed_report(7, "weekly_raw")

        # make monthly raw report
        _generate_windowed_report(30, "monthly_raw")

    else:
        dump_empty_report(report_date=end_date)

    print(
        f"done generating reports, seconds remaining: {context.get_remaining_time_in_millis() / 1000.0}"
    )

    end = time.time()
    lambda_results = {
        "results_count": len(results),
        "valid_results_count": len(valid_results),
        "invalid_results_count": len(invalid_results),
        "invalid_results": invalid_results,
        "duration": round(end - start, 3),
        "report": f'{end_date.strftime("%Y-%m-%d")}.json',
    }

    print(
        f"all done, seconds remaining: {context.get_remaining_time_in_millis() / 1000.0}"
    )

    print("done orchestrating")
    return lambda_results
