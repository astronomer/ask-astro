# Ask Astro Backend API

## Prerequisites

- Install [Docker](https://docs.docker.com/engine/install/)
- Access to Firestore
- Access to Langchain

## Local development

<!-- TODO: Add steps to setup Firestore-->

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
