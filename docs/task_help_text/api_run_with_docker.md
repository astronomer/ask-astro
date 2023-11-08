```sh
$ poetry run inv api.run-with-docker -h

Usage: inv[oke] [--core-opts] api.run-with-docker [--options] [other tasks here ...]

Docstring:
  Run ask-astro API server with docker

Options:
  -b, --build-image                    build image before run API server
  -c STRING, --container-name=STRING   ask-astro API server container name
  -f, --[no-]follow-logs               follow logs after running container
  -i STRING, --image-name=STRING       ask-astro API server image name
```
