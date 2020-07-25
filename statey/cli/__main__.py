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
@click.option(
    "--show-dag", is_flag=True, help="Show the task graph DAG as part of planning."
)
@click.option(
    "--show-metatasks", is_flag=True, help="Show internal 'meta' tasks like graph and session update opterations.",
)
@click.argument("module")
@click.pass_context
def plan(ctx, module, session_name, show_dag, show_metatasks):
    try:
        module_obj = importlib.import_module(module)
    except ImportError as err:
        click.secho(f'Unable to load module "{module}" with error: {type(err).__name__}: {err}.', fg='red')
        raise click.Abort from err

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
    ctx.obj['show_metatasks'] = show_metatasks

    plan_summary = inspector.plan_summary(plan, show_metatasks)

    summary_string = plan_summary.to_string(ctx.obj["terminal_size"].columns)

    click.echo(summary_string)
    click.echo()

    if show_dag and plan_summary.non_empty_summaries(ctx.obj["terminal_size"].columns):
        task_dag_string = plan_summary.task_dag_string()
        click.secho(f"Task DAG:", fg="green")
        click.echo(task_dag_string)
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
    executor.pm.register(ExecutorLoggingPlugin(ctx.obj['show_metatasks']))

    task_graph = plan.task_graph()

    try:
        click.echo()
        exec_info = executor.execute(task_graph)
        click.echo()

        exec_summary = inspector.execution_summary(exec_info, ctx.obj['show_metatasks'])

        exec_summary_string = exec_summary.to_string()

        click.echo(exec_summary_string)
    finally:
        ctx.obj["state_manager"].dump(task_graph.resource_graph, st.registry)


@cli.command()
@click.option(
    "--show-dag", is_flag=True, help="Show the task graph DAG as part of planning."
)
@click.option(
    "--show-metatasks", is_flag=True, help="Show internal 'meta' tasks like graph and session update opterations.",
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    help="Do not prompt to confirm we want to apply the plan.",
)
@click.pass_context
def down(ctx, show_dag, show_metatasks, yes):
    resource_graph = ctx.obj["state_manager"].load(st.registry)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(refresh_graph(resource_graph))

    migrator = DefaultMigrator()
    # Run plan w/ an empty session
    session = st.create_resource_session()
    plan = migrator.plan(session, resource_graph)

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    ctx.obj["plan"] = plan
    ctx.obj["session"] = session

    plan_summary = inspector.plan_summary(plan, show_metatasks)

    summary_string = plan_summary.to_string(ctx.obj["terminal_size"].columns)

    click.echo(summary_string)
    click.echo()

    is_null = not plan_summary.non_empty_summaries(ctx.obj["terminal_size"].columns)

    if is_null:
        return

    if show_dag:
        task_dag_string = plan_summary.task_dag_string()
        click.secho(f"Task DAG:", fg="green")
        click.echo(task_dag_string)
        click.echo()

    if not (
        yes
        or click.confirm(
            f'Do you want to {click.style("apply", bold=True)} these changes?'
        )
    ):
        raise click.Abort

    executor = AsyncIOGraphExecutor()
    executor.pm.register(ExecutorLoggingPlugin(show_metatasks))

    task_graph = plan.task_graph()

    try:
        click.echo()
        exec_info = executor.execute(task_graph)
        click.echo()

        exec_summary = inspector.execution_summary(exec_info, show_metatasks)

        exec_summary_string = exec_summary.to_string()

        click.echo(exec_summary_string)
    finally:
        ctx.obj["state_manager"].dump(task_graph.resource_graph, st.registry)


@cli.command()
@click.pass_context
def refresh(ctx):
    resource_graph = ctx.obj["state_manager"].load(st.registry)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(refresh_graph(resource_graph))

    click.secho('State refreshed successfully.', fg='green', bold=True)

    ctx.obj["state_manager"].dump(resource_graph, st.registry)


if __name__ == "__main__":
    cli(prog_name="statey", auto_envvar_prefix="STATEY")
