import os
from git.repo.base import Repo
import shutil

# install deps in package
package_name = "python/lib/python3.9/site-packages/"
if os.path.exists(package_name):
    shutil.rmtree(package_name)
else:
    os.makedirs(package_name, exist_ok=True)

# get repo
scraper_repo_link = "https://github.com/josemorenoo/batch_scraper.git"
Repo.clone_from(scraper_repo_link, f"{package_name}batch_scraper")
print(f"cloned repo to  {package_name}batch_scraper")

# download dependencies
with open(f"{package_name}batch_scraper/requirements.txt", "r") as req_file:
    reqs = [r.strip("\n") for r in req_file.readlines()]
    for r in reqs:
        print(r)
        os.system(f"pip3 install  --target ./{package_name} '{r}'")

# create deployment package
if os.path.exists("orchestrator-deployment.zip"):
    os.remove("orchestrator-deployment.zip")
os.system("zip orchestrator-deployment.zip orchestrator_lambda.py")
os.system(f"cd {package_name} && zip -r ../../../../orchestrator-deployment.zip *")

# cleanup
shutil.rmtree("python")

# upload deployment package
os.system(
    "aws s3 cp orchestrator-deployment.zip s3://coincommit/deployment-packages/orchestrator-deployment.zip"
)
os.system(
    f"aws lambda update-function-code --function-name scrape-master --s3-bucket coincommit --s3-key deployment-packages/orchestrator-deployment.zip"
)
