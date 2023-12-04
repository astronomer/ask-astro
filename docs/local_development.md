# Setup Local Development Tool

[TOC]

## Prerequisites

- [Python 3.11](https://www.python.org/downloads/release/python-3116/)
- [Install poetry](https://python-poetry.org/docs/#installation)


## Setup project root poetry envinroment for development tools

```sh
$ pwd

/.../ask-astro

$ python --version

Python 3.11.x

# install poetry (https://python-poetry.org/docs/#installation)
$ python -m pip install poetry
$ poetry install
```

![list_tasks](_static/images/task_help_message/list-tasks.svg)

### Documentation Tasks

- Build sphinx docs

![docs.build](_static/images/task_help_message/docs-build.svg)

- Serve the docs locally (http://127.0.0.1:8000)

![docs.serve](_static/images/task_help_message/docs-serve.svg)

- Generate the screenshot for help message on each tasks

![docs.generate-tasks-help-screenshot](_static/images/task_help_message/docs-generate-tasks-help-screenshot.svg)

### Airflow DAGs Tasks

- Run ask-astro airflow

![airflow.run](_static/images/task_help_message/airflow-run.svg)

- Stop ask-astro airflow

![airflow.stop](Stop ask-astro airflow)

### Backend API Tasks

- Go to [Ask Astro Backend API](./api/README.md)

### UI Tasks

- Go to [Ask-Astro UI](./ui/README.md)


### Run linters

- Run pre-commit checks

![run-pre-commit](_static/images/task_help_message/run-pre-commit.svg)
