from invoke import Collection, task
from invoke.context import Context

from tasks import api, docs, ui
from tasks.common import project_root


@task
def run_pre_commit(ctx: Context) -> None:
    """Run pre-commit"""
    with ctx.cd(project_root):
        ctx.run("pre-commit run --all-files")


ns = Collection()
ns.add_collection(api)
ns.add_collection(ui)
ns.add_collection(docs)
ns.add_task(run_pre_commit)
