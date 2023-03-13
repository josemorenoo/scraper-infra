from datetime import datetime
import os
from git.repo.base import Repo
import shutil
import sys


if __name__ == "__main__":
    # report_date = datetime.today()
    report_date = datetime(2023, 3, 12)
    report_date_str = report_date.strftime("%Y-%m-%d")

    # clone birdbot repo to get summary_generator
    repo_link = "https://github.com/josemorenoo/birdbot.git"
    clone_to = "birdbot"
    if os.path.exists(clone_to):
        shutil.rmtree(clone_to)
    else:
        os.makedirs(clone_to, exist_ok=True)
    Repo.clone_from(repo_link, clone_to)

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

    summary_report_path = generate_summary_report(report_date)
    os.system(f"cp {summary_report_path} coinfront/coincommit/public/summary.json")

    # re-build and deploy page
    os.system(
        f"cd coinfront/coincommit && npm install && npm run build && npm run deploy"
    )

    # cleanup
    os.remove(f"/tmp/{report_date_str}.json")
    os.remove(summary_report_path)
    shutil.rmtree("birdbot")
    shutil.rmtree("coinfront")

    # update lambda
    """
    os.system(
        f"aws lambda update-function-code --function-name birdbot --s3-bucket coincommit --s3-key deployment-packages/birdbot-deployment.zip"
    )

    """
