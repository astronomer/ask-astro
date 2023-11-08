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

```{include} ../task_help_text/api_init_poetry_env.md
```

* Run ask-astro API server with poetry

```{include} ../task_help_text/api_run_with_poetry.md
```

### Run with Docker

* Run ask-astro API server with docker

```{include} ../task_help_text/api_run_with_docker.md
```

* Stop ask-astro API server container

```{include} ../task_help_text/api_stop_container.md
```
