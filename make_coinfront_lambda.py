import os
from git.repo.base import Repo
import shutil

code_file = "lambda_coinfront_updator"
lambda_name = "coinfront-updator"


# install deps in package
package_name = "python/lib/python3.9/site-packages/"
if os.path.exists(package_name):
    shutil.rmtree(package_name)
else:
    os.makedirs(package_name, exist_ok=True)

# clone birdbot repo and setup as a package to get summary_generator
repo_link = "https://github.com/josemorenoo/birdbot.git"
clone_to = f"{package_name}birdbot"
if os.path.exists(clone_to):
    shutil.rmtree(clone_to)
else:
    os.makedirs(clone_to, exist_ok=True)
Repo.clone_from(repo_link, clone_to)

# download dependencies
for r in ["boto3", "requests"]:
    os.system(f"pip3 install --target ./{package_name} {r}")


# create deployment package, single file
if os.path.exists(f"{lambda_name}.zip"):
    os.remove(f"{lambda_name}.zip")
os.system(f"zip {lambda_name}.zip {code_file}.py")
os.system(f"cd {package_name} && zip -r ../../../../{lambda_name}.zip *")

# upload deployment package
os.system(
    f"aws s3 cp {lambda_name}.zip s3://coincommit/deployment-packages/{lambda_name}.zip"
)
os.system(
    f"aws lambda update-function-code --function-name {lambda_name} --s3-bucket coincommit --s3-key deployment-packages/{lambda_name}.zip"
)
