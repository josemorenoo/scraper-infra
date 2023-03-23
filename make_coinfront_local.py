from datetime import datetime
import json
import os
from git.repo.base import Repo
import shutil
import sys

from scripts.create_weekly_reports import download_combine_weekly_json_files


if __name__ == "__main__":
    report_date = datetime.today()
    report_date_str = report_date.strftime("%Y-%m-%d")

    # clone birdbot repo to get summary_generator
    repo_link = "https://github.com/josemorenoo/birdbot.git"
    clone_to = "birdbot"
    if os.path.exists(clone_to):
        shutil.rmtree(clone_to)
    else:
        os.makedirs(clone_to, exist_ok=True)
    Repo.clone_from(repo_link, clone_to)
    os.system("cp config/local_bird_config.json birdbot/config/local_bird_config.json")

    # get coinfront repo
    repo_link = "https://github.com/josemorenoo/coinfront.git"
    clone_to = "coinfront"
    if os.path.exists(clone_to):
        shutil.rmtree(clone_to)
    else:
        os.makedirs(clone_to, exist_ok=True)
    Repo.clone_from(repo_link, clone_to)

    # pull down existing daily report
    os.system(
        f"aws s3 cp s3://coincommit/reports/{report_date_str}/{report_date_str}.json /tmp/{report_date_str}.json"
    )

    # generate summary report
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "birdbot")))
    from birdbot.report_parser.report_util import generate_summary_report

    # make daily summary
    summary_report_path = generate_summary_report(report_date, mode="DAILY")
    print(summary_report_path)
    if os.path.exists(summary_report_path):
        os.system(f"cp {summary_report_path} coinfront/coincommit/src/summary.json")
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
        os.system(
            f"cp {weekly_summary_report_path} coinfront/coincommit/src/weekly_summary.json"
        )
    else:
        raise Exception(
            f"weekly_summary.json did not exist at {weekly_summary_report_path} oh nonoooo"
        )

    # timestamp the build
    timestamp_path = "/tmp/updated_on.json"

    with open(timestamp_path, "w") as fp:
        json.dump({"date": report_date_str}, fp)

    os.system(f"cp {timestamp_path} coinfront/coincommit/src/updated_on.json")

    # empty bucket then re-build and deploy page
    os.system(
        f"aws s3 rm s3://coinfront --recursive && cd coinfront/coincommit && npm install && npm run build && npm run deploy"
    )

    # upload summaries
    os.system(
        f"aws s3 cp {weekly_summary_report_path} s3://coinfront/assets/weekly_summary.json"
    )
    os.system(f"aws s3 cp {summary_report_path} s3://coinfront/assets/summary.json")

    # cleanup
    os.remove(f"/tmp/{report_date_str}.json")
    os.remove(summary_report_path)
    os.remove(weekly_aggregation_path)
    os.remove(timestamp_path)
    shutil.rmtree("birdbot")
    shutil.rmtree("coinfront")
