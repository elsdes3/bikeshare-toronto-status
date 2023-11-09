# Bikeshre Status Retrieval Workflow

![CI](https://github.com/elsdes3/bikeshare-toronto-status/workflows/CI/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/mit)
![OpenSource](https://badgen.net/badge/Open%20Source%20%3F/Yes%21/blue?icon=github)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
![prs-welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)

## [About](#about)
Query public API to get station status for the Bike Share Toronto network.

## [Usage](#usage)
The deployment workflow here follows the Prefect [`Deployment` API for Python](https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow) using the [`.build_from_flow()` method](https://docs.prefect.io/latest/api-ref/prefect/deployments/deployments/#prefect.deployments.deployments.Deployment).

### [Manual](#manual)
1. Terminals 1 and 2
   - configure Prefect Cloud
     ```bash
     prefect profile use cloud
     ```
   - change into the `deployments` folder
     ```bash
     cd deployments
     ```
2. (terminal 1) create deployment
   ```bash
   python3 deployment.py
   ```
3. (terminal 1) start agent
   ```bash
   prefect agent start -q <queue-name>
   ```
4. (terminal 2) run deployment
   ```bash
   prefect deployment run <deployment-name>
   ```

### [Using `tox` Environments](#using-tox-environments)
1. (terminal 1) create deployment
   ```bash
   make docker-create
   ```
2. (terminal 1) start agent
   ```bash
   make agent
   ```
3. (terminal 2) run deployment
   ```bash
   make deploy-run
   ```

Using `.tox`, the following commands are run before executing any Prefect command
```bash
$ cd deployments
$ prefect profile use cloud
```

## [Project Organization](#project-organization)

    ├── .gitignore                    <- files and folders to be ignored by version control system
    ├── .pre-commit-config.yaml       <- configuration file for pre-commit hooks
    ├── .github
    │   ├── workflows
    │       └── main.yml              <- configuration file for CI build on Github Actions
    ├── data
    ├── LICENSE
    ├── Makefile                      <- Makefile with commands like `make lint` or `make build`
    ├── README.md                     <- The top-level README for developers using this project.
    └── tox.ini                       <- tox file with settings for running tox; see https://tox.readthedocs.io/en/latest/
