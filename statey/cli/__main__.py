import asyncio
import click
import json
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


async def refresh_graph(graph, finalize: bool = False):
    with click.progressbar(
        length=len(graph.graph.nodes),
        label=click.style("Refreshing state...", fg="yellow"),
    ) as bar:
        async for key in graph.refresh(st.registry, finalize):
            bar.update(1)


session_name_opt = click.option(
    "--session-name",
    help="Set a module attribute other than `session` to use for planning.",
    default="session()",
)

task_dag_opt = click.option(
    "--task-dag", is_flag=True, help="Show the task graph DAG as part of planning."
)

metatasks_opt = click.option(
    "--metatasks",
    is_flag=True,
    help="Show internal 'meta' tasks like graph and session update opterations.",
)

yes_opt = click.option(
    "-y",
    "--yes",
    is_flag=True,
    help="Do not prompt to confirm we want to apply the plan.",
)

task_heartbeat_opt = click.option(
    "--task-heartbeat",
    type=float,
    default=15.0,
    help="Specify a heartbeat interval to use when executing tasks.",
)

full_trace_opt = click.option(
    '--fulltrace',
    is_flag=True,
    help="Print full stack traces for task errors."
)


@cli.group(invoke_without_command=True)
@session_name_opt
@task_dag_opt
@metatasks_opt
@click.argument("module")
@click.pass_context
def plan(ctx, module, session_name, task_dag, metatasks):
    try:
        module_obj = importlib.import_module(module)
    except ImportError as err:
        click.secho(
            f'Unable to load module "{module}" with error: {type(err).__name__}: {err}.',
            fg="red",
        )
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
    plan = loop.run_until_complete(migrator.plan(session, resource_graph))

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    ctx.obj["plan"] = plan
    ctx.obj["module_name"] = module
    ctx.obj["module"] = module_obj
    ctx.obj["session_name"] = session_name
    ctx.obj["session"] = session
    ctx.obj["metatasks"] = metatasks

    plan_summary = inspector.plan_summary(plan, metatasks)

    summary_string = plan_summary.to_string(ctx.obj["terminal_size"].columns)

    click.echo(summary_string)
    click.echo()

    if task_dag and plan_summary.non_empty_summaries(ctx.obj["terminal_size"].columns):
        task_dag_string = plan_summary.task_dag_string()
        click.secho(f"Task DAG:", fg="green")
        click.echo(task_dag_string)
        click.echo()


@plan.command()
@yes_opt
@task_heartbeat_opt
@full_trace_opt
@click.pass_context
def up(ctx, yes, task_heartbeat, fulltrace):
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
    executor.pm.register(ExecutorLoggingPlugin(ctx.obj["metatasks"], task_heartbeat))

    task_graph = plan.task_graph()

    try:
        click.echo()
        exec_info = executor.execute(task_graph)
        click.echo()

        exec_summary = inspector.execution_summary(exec_info, ctx.obj["metatasks"])

        exec_summary_string = exec_summary.to_string(full_trace=fulltrace)

        click.echo(exec_summary_string)
    finally:
        ctx.obj["state_manager"].dump(task_graph.resource_graph, st.registry)


@cli.command()
@task_dag_opt
@metatasks_opt
@yes_opt
@task_heartbeat_opt
@full_trace_opt
@click.pass_context
def down(ctx, task_dag, metatasks, yes, task_heartbeat, fulltrace):
    resource_graph = ctx.obj["state_manager"].load(st.registry)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(refresh_graph(resource_graph))

    migrator = DefaultMigrator()
    # Run plan w/ an empty session
    session = st.create_resource_session()
    loop = asyncio.get_event_loop()
    plan = loop.run_until_complete(migrator.plan(session, resource_graph))

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    ctx.obj["plan"] = plan
    ctx.obj["session"] = session

    plan_summary = inspector.plan_summary(plan, metatasks)

    summary_string = plan_summary.to_string(ctx.obj["terminal_size"].columns)

    click.echo(summary_string)
    click.echo()

    is_null = not plan_summary.non_empty_summaries(ctx.obj["terminal_size"].columns)

    if is_null:
        return

    if task_dag:
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
    executor.pm.register(ExecutorLoggingPlugin(metatasks, task_heartbeat))

    task_graph = plan.task_graph()

    try:
        click.echo()
        exec_info = executor.execute(task_graph)
        click.echo()

        exec_summary = inspector.execution_summary(exec_info, metatasks)

        exec_summary_string = exec_summary.to_string(full_trace=fulltrace)

        click.echo(exec_summary_string)
    finally:
        ctx.obj["state_manager"].dump(task_graph.resource_graph, st.registry)


@cli.command()
@click.pass_context
def refresh(ctx):
    resource_graph = ctx.obj["state_manager"].load(st.registry)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(refresh_graph(resource_graph, True))

    click.secho("State refreshed successfully.", fg="green", bold=True)

    ctx.obj["state_manager"].dump(resource_graph, st.registry)


@cli.command()
@click.option('--compact', is_flag=True, help='Do not pretty print JSON output.')
@click.argument('paths', nargs=-1)
@click.pass_context
def query(ctx, paths, compact):
    resource_graph = ctx.obj["state_manager"].load(st.registry)
    path_parser = st.PathParser()

    session = st.create_session()

    for path in paths:
        head, *tail = path_parser.split(path)
        if head not in resource_graph.graph.nodes:
            click.secho(f'{path} does not exist.', fg='red')
            raise click.Abort

        node = resource_graph.graph.nodes[head]
        obj = st.Object(node['value'], node['type'])
        for comp in tail:
            obj = obj[comp]

        if compact:
            print(json.dumps(session.resolve(obj)))
        else:
            print(json.dumps(session.resolve(obj), indent=2, sort_keys=True))


if __name__ == "__main__":
    cli(prog_name="statey", auto_envvar_prefix="STATEY")
