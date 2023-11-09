from pathlib import Path

from invoke import task
from invoke.context import Context

from tasks.common import project_root

api_root = project_root / Path("api")

api_version = "1.0.0-dev"
api_image_name = "ask-astro-api"
api_container_name = "ask-astro-api"


@task
def init_poetry_env(ctx: Context) -> None:
    """Initialize the ask-astro API server local poetry environment"""
    with ctx.cd(api_root):
        print("Initialize ask-astro API local poetry environment")
        ctx.run("poetry install")


@task(help={"init": "initialize poetry environment before running server"})
def run_with_poetry(ctx: Context, init: bool = False) -> None:
    """Run ask-astro API server with poetry"""
    with ctx.cd(api_root):
        if init:
            init_poetry_env(ctx)
        print("Starting ask-astro API local poetry environment")
        ctx.run("poetry run python -m ask_astro.app")


@task(
    help={
        "build_image": "build image before run API server",
        "image_name": "ask-astro API server image name",
        "container_name": "ask-astro API server container name",
        "follow_logs": "follow logs after running container",
    }
)
def run_with_docker(
    ctx: Context,
    build_image: bool = False,
    image_name: str = f"{api_image_name}:{api_version}",
    container_name: str = api_container_name,
    follow_logs: bool = True,
) -> None:
    """Run ask-astro API server with docker"""
    with ctx.cd(api_root):
        if build_image:
            print(f"Building image {image_name}")
            ctx.run(f"docker build . --tag {image_name}")

        ctx.run(f"docker run --name {container_name} -p 8080:8080 --detach {image_name}")
        if follow_logs:
            ctx.run(f"docker logs {container_name} -f")


@task(
    help={"container_name": "ask-astro API server container name", "remove_container": "remove container after stopped"}
)
def stop_container(ctx: Context, container_name: str = api_container_name, remove_container: bool = True) -> None:
    """Stop ask-astro API server container"""
    with ctx.cd(api_root):
        print(f"stop container {container_name}")
        ctx.run(f"docker stop {container_name}")
        if remove_container:
            print(f"remove container {container_name}")
            ctx.run(f"docker remove {container_name}")


@task
def test(ctx: Context) -> None:
    """Run ask-astro API tests"""
    with ctx.cd(api_root):
        print("Run ask-astro API tests")
        ctx.run("poetry run ../tests || poetry run pytest --last-failed ../tests")
