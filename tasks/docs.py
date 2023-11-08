from pathlib import Path

from invoke import task
from invoke.context import Context

from tasks.common import project_root

docs_root = project_root / Path("docs")


@task(help={"clean": "clean the docs before building"})
def build(ctx: Context, clean: bool = False) -> None:
    """Build sphinx docs"""
    with ctx.cd(docs_root):
        if clean:
            ctx.run("make clean")
        ctx.run("make html")


@task(help={"rebuild": "clean and build the doc before serving"})
def serve(ctx: Context, rebuild: bool = False) -> None:
    """Serve the docs locally (http://127.0.0.1:8000)"""
    with ctx.cd(docs_root / Path("_build/html")):
        if rebuild:
            build(ctx, clean=True)
        ctx.run("python -m http.server")
