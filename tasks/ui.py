from pathlib import Path

from invoke import task
from invoke.context import Context

from tasks.common import project_root

ui_root = project_root / Path("ui")


@task(name="init")
def init_task(ctx: Context) -> None:
    """Initialize UI server dependencies"""
    with ctx.cd(ui_root):
        ctx.run("npm install")


@task(help={"init": "init UI dev env", "open_browser": "open the browser after running the server"})
def run(ctx: Context, init: bool = False, open_browser: bool = False) -> None:
    """Run UI server"""
    with ctx.cd(ui_root):
        if init:
            init_task(ctx)
        cmd = "npm run dev"
        if open_browser:
            cmd = f"{cmd} -- --open"
        ctx.run(cmd)
