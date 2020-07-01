import asyncio
import click
import importlib
import shutil

import statey as st
from statey.executor import AsyncIOGraphExecutor
from statey.plan import DefaultMigrator
from statey.cli.graph_utils import Inspector, ExecutorLoggingPlugin
from statey.cli.state_manager import FileStateManager


inspector = Inspector()


@click.group()
@click.option("--state", help="File to use to save state.", default="state.json")
@click.pass_context
def cli(ctx, state):
    ctx.ensure_object(dict)
    ctx.obj["state_manager"] = FileStateManager(state)
    ctx.obj["terminal_size"] = shutil.get_terminal_size((80, 20))
    # Set up all default plugins
    # register_default_plugins()


async def refresh_graph(graph):
    with click.progressbar(
        length=len(graph.graph.nodes),
        label=click.style("Refreshing state...", fg="yellow"),
    ) as bar:
        async for key in graph.refresh(st.registry):
            bar.update(1)


@cli.group(invoke_without_command=True)
@click.option(
    "--session-name",
    help="Set a module attribute other than `session` to use for planning.",
    default="session()",
)
@click.argument("module")
@click.pass_context
def plan(ctx, module, session_name):
    module_obj = importlib.import_module(module)

    if session_name.endswith("()"):
        session_factory = getattr(module_obj, session_name[:-2])
        session = session_factory()
    else:
        session = getattr(module_obj, session_name)

    resource_graph = ctx.obj["state_manager"].load(st.registry)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(refresh_graph(resource_graph))

    click.echo(
        f'Loaded {click.style(session_name, fg="green", bold=True)} from '
        f'{click.style(module, fg="green", bold=True)} successfully.'
    )

    migrator = DefaultMigrator()
    plan = migrator.plan(session, resource_graph)

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    ctx.obj["plan"] = plan
    ctx.obj["module_name"] = module
    ctx.obj["module"] = module_obj
    ctx.obj["session_name"] = session_name
    ctx.obj["session"] = session

    plan_summary = inspector.plan_summary(plan)

    summary_string = plan_summary.to_string(ctx.obj["terminal_size"].columns)

    click.echo(summary_string)
    click.echo()


@plan.command()
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    help="Do not prompt to confirm we want to apply the plan.",
)
@click.pass_context
def up(ctx, yes):
    plan = ctx.obj["plan"]

    # Sort of hacky way to check if the plan is empty--executing would only update the state.
    if not inspector.plan_summary(plan).non_empty_summaries(
        ctx.obj["terminal_size"].columns
    ):
        return

    if not (
        yes
        or click.confirm(
            f'Do you want to {click.style("apply", bold=True)} these changes?'
        )
    ):
        raise click.Abort

    executor = AsyncIOGraphExecutor()
    executor.pm.register(ExecutorLoggingPlugin())

    task_graph = plan.task_graph()

    try:
        click.echo()
        exec_info = executor.execute(task_graph)
        click.echo()

        exec_summary = inspector.execution_summary(exec_info)

        exec_summary_string = exec_summary.to_string()

        click.echo(exec_summary_string)
    finally:
        ctx.obj["state_manager"].dump(task_graph.resource_graph, st.registry)


if __name__ == "__main__":
    cli(prog_name="statey", auto_envvar_prefix="STATEY")
