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
scraper_repo_link = "https://github.com/josemorenoo/birdbot.git"
Repo.clone_from(scraper_repo_link, f"{package_name}birdbot")
print(f"cloned repo to  {package_name}birdbot")

# download dependencies
with open(f"{package_name}birdbot/requirements.txt", "r") as req_file:
    reqs = [r.strip("\n") for r in req_file.readlines()]
    for r in reqs:
        install = f"python3.9 -m pip install --target ./{package_name} {r}"
        os.system(install)
#os.system(f"rm -rf ./{package_name}plotly/express")

# create deployment package
if os.path.exists("birdbot-deployment.zip"):
    os.remove("birdbot-deployment.zip")
os.system("zip birdbot-deployment.zip birdbot_lambda.py")
os.system(f"cd {package_name} && zip -r ../../../../birdbot-deployment.zip *")

# cleanup
shutil.rmtree("python")

# upload deployment package
os.system(
    "aws s3 cp birdbot-deployment.zip s3://coincommit/deployment-packages/birdbot-deployment.zip"
)
os.system(
    f"aws lambda update-function-code --function-name birdbot --s3-bucket coincommit --s3-key deployment-packages/birdbot-deployment.zip"
)
