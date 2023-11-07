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

```{include} task_help_text/list_tasks.md
```

### Documentation Tasks

* Build sphinx docs

```{include} task_help_text/docs_build.md
```


* Serve the docs locally (http://127.0.0.1:8000)

```{include} task_help_text/docs_serve.md
```


### Backend API Tasks

- Go to [Ask Astro Backend API](./api/README.md)
