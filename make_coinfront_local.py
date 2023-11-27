from datetime import datetime, timedelta
import json
import os
import shutil
import sys
import time
import boto3

from scripts.create_weekly_reports import download_combine_weekly_json_files
from scripts.create_monthly_reports import download_combine_monthly_json_files


if __name__ == "__main__":
    s3 = boto3.resource("s3")
    s3_client = boto3.client("s3")

    report_date = datetime.today()
    report_date_str = report_date.strftime("%Y-%m-%d")
    # report_date_str = "2023-03-24"
    # report_date = datetime(2023, 3, 24)
    print(f"today is: {report_date_str}")

    if os.path.exists("/tmp/coinfront"):
        shutil.rmtree("/tmp/coinfront")

    usr = "/home/ec2-user"

    # get birdbot repo
    repo_link = "git@github.com:josemorenoo/birdbot.git"
    clone_to = f"{usr}/scraper-infra/birdbot"
    if os.path.exists(clone_to):
        shutil.rmtree(clone_to)
    else:
        os.makedirs(clone_to, exist_ok=True)
    # Repo.clone_from(repo_link, clone_to)
    os.system(f"git clone {repo_link} {clone_to}")
    os.system(
        f"cp config/local_bird_config.json {clone_to}/config/local_bird_config.json"
    )

    # get coinfront repo
    repo_link = "git@github.com:josemorenoo/coinfront.git"
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
    weekly_report_local_path = f"/tmp/weekly_raw.json"
    monthly_report_local_path = f"/tmp/monthly_raw.json"


    print(f"downloading s3://{bucket}/{s3_object} to {daily_report_local_path}")
    s3_client.download_file(bucket, s3_object, daily_report_local_path)
    s3_client.download_file(bucket, f"reports/{report_date_str}/weekly_raw.json", weekly_report_local_path)
    s3_client.download_file(bucket, f"reports/{report_date_str}/monthly_raw.json", monthly_report_local_path)

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
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "birdbot")))

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

    # make monthly summary
    monthly_aggregation_path = download_combine_monthly_json_files(report_date)
    monthly_summary_report_path = generate_summary_report(
        report_date, monthly_aggregation_path, mode="MONTHLY"
    )
    if os.path.exists(monthly_summary_report_path):
        print("monthly aggregation generated at", monthly_summary_report_path)
        os.system(
            f"cp {monthly_summary_report_path} /tmp/coinfront/coincommit/src/monthly_summary.json"
        )
    else:
        raise Exception(
            f"monthly_summary.json did not exist at {monthly_summary_report_path} oh nonoooo"
        )

    # timestamp the build
    timestamp_path = "/tmp/updated_on.json"

    with open(timestamp_path, "w") as fp:
        json.dump({"date": report_date_str}, fp)

    os.system(f"cp {timestamp_path} /tmp/coinfront/coincommit/src/updated_on.json")
    os.system(
        f"cp {daily_report_local_path} /tmp/coinfront/coincommit/src/daily_raw.json"
    )
    os.system(
        f"cp {weekly_report_local_path} /tmp/coinfront/coincommit/src/weekly_raw.json"
    )
    os.system(
        f"cp {monthly_report_local_path} /tmp/coinfront/coincommit/src/monthly_raw.json"
    )

    # empty bucket then re-build and deploy page
    s3.Bucket("coinfront").objects.all().delete()
    os.system(
        f"cd /tmp/coinfront/coincommit && npm install && npm run build && npm run deploy"
    )

    # upload raw data
    s3_client.upload_file(daily_report_local_path, "coinfront", "assets/daily_raw.json")
    s3_client.upload_file(weekly_report_local_path, "coinfront", "assets/weekly_raw.json")
    s3_client.upload_file(monthly_report_local_path, "coinfront", "assets/monthly_raw.json")

    # upload summaries
    s3_client.upload_file(
        weekly_summary_report_path, "coinfront", "assets/weekly_summary.json"
    )
    s3_client.upload_file(
        monthly_summary_report_path, "coinfront", "assets/monthly_summary.json"
    )
    s3_client.upload_file(summary_report_path, "coinfront", "assets/summary.json")

    # cleanup
    os.remove(f"/tmp/{report_date_str}.json")
    os.remove(summary_report_path)
    os.remove(weekly_aggregation_path)
    os.remove(timestamp_path)
    if os.path.exists("/home/ec2-user/scraper-infra/birdbot"):
        shutil.rmtree("/home/ec2-user/scraper-infra/birdbot")
    if os.path.exists("/tmp/coinfront"):
        shutil.rmtree("/tmp/coinfront")
