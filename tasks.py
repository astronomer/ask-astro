from invoke import task
from invoke.context import Context

api_version = "1.0.0-dev"
api_image_name = "ask-astro-api"
api_container_name = "ask-astro-api"


@task
def run_api_server_with_poetry(ctx: Context) -> None:
    with ctx.cd("api"):
        ctx.run("poetry run python -m ask_astro.app")


@task
def run_api_server(
    ctx: Context,
    build_image: bool = False,
    image_name: str = f"{api_image_name}:{api_version}",
    container_name: str = api_container_name,
    follow_logs: bool = True,
) -> None:
    with ctx.cd("api"):
        if build_image:
            print(f"Building image {image_name}")
            ctx.run(f"docker build . --tag {image_name}")

        ctx.run(f"docker run --name {container_name} -p 8080:8080 --detach {image_name}")
        if follow_logs:
            ctx.run(f"docker logs {container_name} -f")


@task
def stop_api_server(ctx: Context, container_name: str = api_container_name, remove_container: bool = True):
    with ctx.cd("api"):
        print(f"stop container {container_name}")
        ctx.run(f"docker stop {container_name}")
        if remove_container:
            print(f"remove container {container_name}")
            ctx.run(f"docker remove {container_name}")
