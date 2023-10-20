# Ask Astro Backend API

## Prerequisites

- Install [Docker](https://docs.docker.com/engine/install/)
- Access to Firestore
- Access to Langchain
- Python 3.11
- [Install poetry](https://python-poetry.org/docs/#installation)

## Local development

<!-- TODO: Add steps to setup Firestore-->

### Setup project root poetry envinroment for development tools

```sh
$ pwd

/.../ask-astro

$ python --version

Python 3.11.x

# install poetry (https://python-poetry.org/docs/#installation)
$ python -m pip install poetry
$ poetry install

# shows all the commands we have for local development
$ poetry run inv -l

Available tasks:

  init-api-server-poetry-env   Initialize the ask-astro API local poetry environment
  run-api-server-with-docker   Run ask-astro API server with docker
  run-api-server-with-poetry   Run ask-astro API server with poetry
  stop-api-server-container    Stop ask-astro API server container
```

### Setup local development environment

Build the API server

```bash
docker build . --tag <IMAGE>
```

Generate Env Variable template and add appropriate values

```bash
python3 scripts/local_dev.py api-env-template
```

Run the API server

```bash
docker run --env-file ./.env -p 8080:8080 <IMAGE>
```
