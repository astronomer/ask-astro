# Ask Astro Backend API

## Prerequisites

- Install [Docker](https://docs.docker.com/engine/install/)
- Access to Firestore
- Access to Langchain
- [Setup Local Development Tools](../local_development.md)

### Setup local development environment

Generate Env Variable template and add appropriate values

```bash
python3 scripts/local_dev.py api-env-template
```

#### Run with poetry
* Initialize the ask-astro API local poetry environment

![api.init-poetry-env](../_static/images/task_help_message/api-init-poetry-env.svg)

* Run ask-astro API server with poetry

![api.run-with-poetry](../_static/images/task_help_message/api-run-with-poetry.svg)

### Run with Docker

* Run ask-astro API server with docker

![api.run-with-docker](../_static/images/task_help_message/api-run-with-docker.svg)

* Stop ask-astro API server container

![api.stop-container](../_static/images/task_help_message/api-stop-container.svg)
