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

# shows all the commands we have for local development
$ poetry run inv -l

Available tasks:

  build-docs                   Build sphinx docs
  init-api-server-poetry-env   Initialize the ask-astro API local poetry environment
  run-api-server-with-docker   Run ask-astro API server with docker
  run-api-server-with-poetry   Run ask-astro API server with poetry
  serve-docs                   Serve the docs locally (http://127.0.0.1:8000)
  stop-api-server-container    Stop ask-astro API server container
```

### Documentation Tasks

* Build sphinx docs

```sh
$ poetry run inv build-docs -h
Usage: inv[oke] [--core-opts] build-docs [--options] [other tasks here ...]

Docstring:
  Build sphinx docs

Options:
  -c, --clean   clean the docs before building
```


* Serve the docs locally (http://127.0.0.1:8000)

```sh
$ poetry run inv serve-docs -h
Usage: inv[oke] [--core-opts] serve-docs [--options] [other tasks here ...]

Docstring:
  Serve the docs locally (http://127.0.0.1:8000)

Options:
  -r, --rebuild   clean and build the doc before serving
```

### Backend API Tasks

- Go to [Ask Astro Backend API](docs/api/README.md)
