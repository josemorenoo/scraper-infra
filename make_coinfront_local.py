from datetime import datetime, timedelta
import json
import os
import shutil
import sys
import time
import boto3

from scripts.create_weekly_reports import download_combine_weekly_json_files


def download_combine_weekly_json_files(
    end_date,
):

    start_date = end_date - timedelta(days=6)

    weekly_data = {}

    for i in range((end_date - start_date).days + 1):
        datestr = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        s3_object = bucket.Object(f"reports/{datestr}/{datestr}.json")
        try:
            file_content = s3_object.get()["Body"].read().decode("utf-8")
            daily_data = json.loads(file_content)
        except:
            print(f"Error downloading or parsing data for {datestr}")
            continue
        for token, metadata in daily_data.items():
            if token in weekly_data:
                existing_token_data = weekly_data[token]
                existing_token_data["commit_count"] += metadata["commit_count"]
                existing_token_data["lines_of_code"] += metadata["lines_of_code"]
                existing_token_data["commit_messages"].extend(
                    metadata["commit_messages"]
                )
                existing_token_data["distinct_authors"].extend(
                    metadata["distinct_authors"]
                )
                existing_token_data["commit_urls"].extend(metadata["commit_urls"])
                existing_token_data["changed_methods"].extend(
                    metadata["changed_methods"]
                )

                merged_dict = existing_token_data["file_extensions"].copy()
                for filetype, filecount in metadata["file_extensions"].items():
                    if filetype in merged_dict:
                        merged_dict[filetype] += filecount
                    else:
                        merged_dict[filetype] = filecount
                existing_token_data["file_extensions"] = merged_dict

                merged_dict = existing_token_data["loc_changes_by_filetype"].copy()
                for action in ["insertions", "deletions", "net"]:
                    actiondict = merged_dict[action]
                    actiondict2 = metadata["loc_changes_by_filetype"][action]
                    for filetype, filecount in actiondict2.items():
                        if filetype in actiondict:
                            actiondict[filetype] += filecount
                        else:
                            actiondict[filetype] = filecount
                    merged_dict[action] = actiondict
                weekly_data[token]["loc_changes_by_filetype"] = merged_dict
            else:
                weekly_data[token] = metadata

        # de-dupe lists
        for token, metadata in weekly_data.items():
            weekly_data[token]["commit_messages"] = list(
                set(metadata["commit_messages"])
            )
            weekly_data[token]["distinct_authors"] = list(
                set(metadata["distinct_authors"])
            )
            weekly_data[token]["commit_urls"] = list(set(metadata["commit_urls"]))
            weekly_data[token]["changed_methods"] = list(
                set(metadata["changed_methods"])
            )

    weekly_file_content = json.dumps(weekly_data, indent=2)
    weekly_aggregation_path = f"/tmp/weekly.json"
    with open(weekly_aggregation_path, "w") as f:
        f.write(weekly_file_content)

    return weekly_aggregation_path


if __name__ == "__main__":
    s3 = boto3.resource("s3")
    s3_client = boto3.client("s3")

    report_date = datetime.today()
    report_date_str = report_date.strftime("%Y-%m-%d")

    # report_date = datetime.today()
    # report_date_str = report_date.strftime("%Y-%m-%d")
    report_date_str = "2023-03-24"
    report_date = datetime(2023, 3, 24)
    print(f"today is: {report_date_str}")

    if os.path.exists("/tmp/coinfront"):
        shutil.rmtree("/tmp/coinfront")

    # get coinfront repo
    repo_link = "https://github.com/josemorenoo/coinfront.git"
    clone_to = "/tmp/coinfront"
    if os.path.exists(clone_to):
        shutil.rmtree(clone_to)
    else:
        os.makedirs(clone_to, exist_ok=True)
    # Repo.clone_from(repo_link, clone_to)
    os.system(f"git clone {repo_link} {clone_to}")

    clone_time = 0.0
    while not os.path.exists(clone_to):
        clone_time += 0.5
        time.sleep(0.5)
        if clone_time > 30.0:
            print(f"cloning coinfront took more than 30sec, breaking")
            break

    # pull down existing daily report
    bucket = "coincommit"
    s3_object = f"reports/{report_date_str}/{report_date_str}.json"
    daily_report_local_path = f"/tmp/{report_date_str}.json"

    print(f"downloading s3://{bucket}/{s3_object} to {daily_report_local_path}")
    s3_client.download_file(bucket, s3_object, daily_report_local_path)

    print(os.listdir("/tmp"))

    # wait for download to finish
    secs_waited = 0.0
    while not os.path.isfile(daily_report_local_path):
        time.sleep(0.5)
        secs_waited += 0.5
        print(
            f"waiting {secs_waited}sec for s3://{bucket}/{s3_object} to download to {daily_report_local_path}"
        )
        if secs_waited > 10.0:
            raise Exception(
                f"waited ten seconds, couldn't find daily report at {s3_object}"
            )

    # setup birdbot repo to create summary report
    sys.path.append("/home/ec2-user/birdbot")

    from birdbot.report_parser.report_util import generate_summary_report

    # make daily summary
    summary_report_path = generate_summary_report(
        report_date=report_date,  # IGNORED, daily_report_local_path is used
        report_path=daily_report_local_path,
        mode="DAILY",
    )
    print("summary report generated at: ", summary_report_path)
    if os.path.exists(summary_report_path):
        os.system(
            f"cp {summary_report_path} /tmp/coinfront/coincommit/src/summary.json"
        )
    else:
        raise Exception(
            f"summary.json did not exist at {summary_report_path} oh nonoooo"
        )

    # make weekly summary
    weekly_aggregation_path = download_combine_weekly_json_files(report_date)
    weekly_summary_report_path = generate_summary_report(
        report_date, weekly_aggregation_path, mode="WEEKLY"
    )
    if os.path.exists(weekly_summary_report_path):
        print("weekly aggregation generated at", weekly_summary_report_path)
        os.system(
            f"cp {weekly_summary_report_path} /tmp/coinfront/coincommit/src/weekly_summary.json"
        )
    else:
        raise Exception(
            f"weekly_summary.json did not exist at {weekly_summary_report_path} oh nonoooo"
        )

    # timestamp the build
    timestamp_path = "/tmp/updated_on.json"

    with open(timestamp_path, "w") as fp:
        json.dump({"date": report_date_str}, fp)

    os.system(f"cp {timestamp_path} /tmp/coinfront/coincommit/src/updated_on.json")

    # empty bucket then re-build and deploy page
    s3.Bucket("coinfront").objects.all().delete()
    os.system(
        f"cd /tmp/coinfront/coincommit && npm install && npm run build && npm run deploy"
    )

    # upload summaries
    s3_client.upload_file(
        daily_report_local_path, "coinfront", "assets/weekly_summary.json"
    )
    s3_client.upload_file(summary_report_path, "coinfront", "assets/summary.json")

    # cleanup
    os.remove(f"/tmp/{report_date_str}.json")
    os.remove(summary_report_path)
    os.remove(weekly_aggregation_path)
    os.remove(timestamp_path)
    if os.path.exists("/tmp/coinfront"):
        shutil.rmtree("/tmp/coinfront")
