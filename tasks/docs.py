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


def _export_cmd_as_svg(ctx: Context, cmd: str, file_name: str) -> None:
    stdout = ctx.run(cmd, hide="both").stdout
    console = Console(record=True, width=80)
    console.print(f"$ {cmd}\n{stdout}")
    console.save_svg(file_name, title="")


@task
def generate_tasks_help_screenshot(ctx: Context) -> None:
    """Generate the screenshot for help message on each tasks"""
    with ctx.cd(project_root):
        image_dir = f"{project_root}/docs/_static/images/task_help_message/"
        list_tasks_cmd = "poetry run inv -l"
        _export_cmd_as_svg(ctx, list_tasks_cmd, f"{image_dir}/list-tasks.svg")

        stdout = ctx.run(list_tasks_cmd, hide="both").stdout
        task_names = [
            message.strip().split()[0]
            for message in filter(lambda s: not s.startswith("   "), stdout.split("\n")[2:-2])
        ]
        for task_name in task_names:
            cmd = f"poetry run inv {task_name} -h"
            file_name = task_name.replace(".", "-")
            _export_cmd_as_svg(ctx, cmd, f"{image_dir}/{file_name}.svg")
