# Coin Commit Scraper Infrastructure

This repository contains the infrastructure code for the Coin Commit scraper, orchestrator, manifest manager, and Birdbot. It includes needed AWS Lambda functions, helper scripts, and assets for managing the coin commit services.

## Table of Contents

- [Directory structure](#directory-structure)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Directory structure

- `scripts/` - Contains helper scripts to create weekly and monthly reports.
- `.gitignore` - List of files and directories to ignore in the git repository.
- `birdbot_lambda.py` - AWS Lambda function for Birdbot.
- `make_birdbot.py` - Script to create a deployment package for the Birdbot Lambda function.
- `make_coinfront_local.py` - Script to create local Coinfront repo and assets.
- `make_manifest_manager.py` - Script to create a deployment package for the Manifest Manager Lambda function.
- `make_orchestrator.py` - Script to create a deployment package for the Orchestrator Lambda function.
- `make_worker.py` - Script to create a deployment package for the Worker Lambda function.
- `repo_manifest_lambda.py` - AWS Lambda function for Repository Manifest Manager.
- `requirements.txt` - Required packages for the project.
- `worker_lambda.py` - AWS Lambda function for workers in the scraper.

## Installation

1. Clone the repository:

```bash
git clone https://github.com/josemorenoo/batch_scraper.git
```

2. Install the required packages:

```bash
pip install -r requirements.txt
```

## Usage

To create deployment packages for the Lambda functions and update them in AWS, run the following scripts:

1. For Birdbot Lambda function:

```bash
python make_birdbot.py
```

2. For Manifest Manager Lambda function:

```bash
python make_manifest_manager.py
```

3. For Orchestrator Lambda function:

```bash
python make_orchestrator.py
```

4. For Worker Lambda function:

```bash
python make_worker.py
```

To create local Coinfront repo and assets, run:

```bash
python make_coinfront_local.py
```

To generate weekly and monthly reports, use the scripts in the `scripts/` directory:

1. For weekly reports:

```bash
python scripts/create_weekly_reports.py
```

2. For monthly reports:

```bash
python scripts/create_monthly_reports.py
```

## License

This project is licensed under the MIT License.
