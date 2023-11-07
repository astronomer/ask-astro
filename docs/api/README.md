# Ask Astro Backend API

## Prerequisites

- Install [Docker](https://docs.docker.com/engine/install/)
- Access to Firestore
- Access to Langchain
- [Setup Local Development Tools](docs/local_development.md)

### Setup local development environment

Generate Env Variable template and add appropriate values

```bash
python3 scripts/local_dev.py api-env-template
```

#### Run with poetry
* Initialize the ask-astro API local poetry environment

```sh
poetry run inv init-api-server-poetry-env
```

* Run ask-astro API server with poetry

```sh
$ poetry run inv run-api-server-with-poetry -h

Usage: inv[oke] [--core-opts] run-api-server-with-poetry [--options] [other tasks here ...]

Docstring:
  Run ask-astro API server with poetry

Options:
  -i, --init   initialize poetry environment before running server
```

### Run with Docker

* Run ask-astro API server with docker

```sh
$ poetry run inv run-api-server-with-docker -h

Usage: inv[oke] [--core-opts] run-api-server-with-docker [--options] [other tasks here ...]

Docstring:
  Run ask-astro API server with docker

Options:
  -b, --build-image                    build image before run API server
  -c STRING, --container-name=STRING   ask-astro API server container name
  -f, --[no-]follow-logs               follow logs after running container
  -i STRING, --image-name=STRING       ask-astro API server image name

```

* Stop ask-astro API server container

```sh
$ poetry run inv stop-api-server-container -h

Usage: inv[oke] [--core-opts] stop-api-server-container [--options] [other tasks here ...]

Docstring:
  Stop ask-astro API server container

Options:
  -c STRING, --container-name=STRING   ask-astro API server container name
  -r, --[no-]remove-container          remove container after stopped
```
