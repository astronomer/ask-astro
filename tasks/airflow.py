from pathlib import Path

from invoke import task
from invoke.context import Context

from tasks.common import project_root

airflow_root = project_root / Path("airflow")


@task
def run(ctx: Context) -> None:
    """Run ask-astro airflow"""
    with ctx.cd(airflow_root):
        print("Starting ask-astro airflow")
        ctx.run("astro dev start")


@task
def stop(ctx: Context) -> None:
    """Stop ask-astro airflow"""
    with ctx.cd(airflow_root):
        print("Starting ask-astro airflow")
        ctx.run("astro dev stop")
