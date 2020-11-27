import logging
import warnings

import click
import shutil

import statey as st
from statey.cli.controller import Controller


LOGGER = logging.getLogger(__name__)


# task_dag_opt = click.option(
#     "--task-dag", is_flag=True, help="Show the task graph DAG as part of planning."
# )

diff_opt = click.option(
    "--diff",
    is_flag=True,
    help="Display full diff for resources instead of just the task DAG.",
)

full_trace_opt = click.option(
    "--fulltrace", is_flag=True, help="Print full stack traces for task errors."
)

yes_opt = click.option(
    "-y",
    "--yes",
    is_flag=True,
    help="Do not prompt to confirm we want to apply the plan.",
)


def run_plan_and_apply(controller, yes):
    """
    Run logic to plan and optionally 
    """
    controller.setup_plan()

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    controller.print_plan_summary()

    if controller.plan.is_empty():
        return

    if not (
        yes
        or click.confirm(
            f'Do you want to {click.style("apply", bold=True)} these changes?'
        )
    ):
        raise click.Abort

    click.echo()
    try:
        controller.execute_plan()
    finally:
        controller.dump_state()

    click.echo()
    controller.print_execution_summary()


@click.group(help="The statey CLI")
@click.option(
    "-c", "--config", help="Configuration file to use", default="statey_conf.py"
)
@click.option("--debug", help="Additional output.", is_flag=True)
@click.pass_context
def cli(ctx, config, debug):
    """

    """
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["terminal_size"] = shutil.get_terminal_size((80, 20))
    ctx.obj["debug"] = debug
    warnings.simplefilter("ignore")


# @task_dag_opt
@cli.command(help="Inspect the potential plan.")
@diff_opt
@click.pass_context
def plan(ctx, diff):
    """

    """
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        show_diff=diff,
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()
    controller.setup_resource_graph()

    controller.setup_session()

    click.echo(
        f'Loaded {click.style(controller.session_name, fg="green", bold=True)} from '
        f'{click.style(controller.module_name, fg="green", bold=True)} successfully.'
    )

    controller.setup_plan()

    click.secho(f"Planning completed successfully.", fg="green")
    click.echo()

    controller.print_plan_summary()


# @task_dag_opt
@cli.command()
@diff_opt
@full_trace_opt
@yes_opt
@click.pass_context
def up(ctx, diff, fulltrace, yes):
    """

    """
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        show_diff=diff,
        fulltrace=fulltrace,
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.setup_resource_graph()

    controller.setup_session()

    click.echo(
        f'Loaded {click.style(controller.session_name, fg="green", bold=True)} from '
        f'{click.style(controller.module_name, fg="green", bold=True)} successfully.'
    )

    run_plan_and_apply(controller, yes)


# @task_dag_opt
@cli.command()
@diff_opt
@full_trace_opt
@yes_opt
@click.pass_context
def down(ctx, diff, fulltrace, yes):
    """

    """
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        show_diff=diff,
        fulltrace=fulltrace,
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.setup_resource_graph()

    controller.session = st.create_resource_session()

    run_plan_and_apply(controller, yes)


@cli.command()
@click.pass_context
def refresh(ctx):
    """

    """
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.refresh_resource_graph()

    click.secho("State refreshed successfully.", fg="green", bold=True)

    controller.dump_state()


@cli.command()
@click.option("--compact", is_flag=True, help="Do not pretty print JSON output.")
@click.argument("paths", nargs=-1)
@click.pass_context
def query(ctx, compact, paths):
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.setup_resource_graph()

    resource_graph = controller.resource_graph
    path_parser = st.PathParser()

    session = st.create_session()

    for path in paths:
        head, *tail = path_parser.split(path)
        if head not in resource_graph.graph.nodes:
            click.secho(f"{path} does not exist.", fg="red")
            raise click.Abort

        node = resource_graph.graph.nodes[head]
        obj = st.Object(node["value"], node["type"])
        for comp in tail:
            obj = obj[comp]

        if compact:
            print(json.dumps(session.resolve(obj)))
        else:
            print(json.dumps(session.resolve(obj), indent=2, sort_keys=True))


@cli.command()
@click.argument("provider")
@click.option("-r", "--resource", type=str, default=None)
@click.pass_context
def docs(ctx, provider, resource):
    """

    """
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.setup_config()

    provider_obj = controller.load_provider(provider)

    if resource is None:
        resource_names = list(provider_obj.list_resources())
        click.echo("\n".join(resource_names))
        return

    resource_obj = controller.load_resource(provider_obj, resource)
    controller.print_resource_docs(resource_obj)


@cli.command(
    help="Entry point to allow installing plugins for various types of providers."
)
@click.argument("requirement", nargs=-1)
@click.option(
    "-r",
    "--requirements-file",
    type=click.File(),
    multiple=True,
    help="Path to a requirements file.",
)
@click.pass_context
def install(ctx, requirement, requirements_file):
    controller = Controller(
        logger=LOGGER,
        terminal_size=ctx.obj["terminal_size"],
        conf_name=ctx.obj["config"],
        debug=ctx.obj["debug"],
    )
    controller.setup_logging()

    controller.setup_config()
    if not (requirement or requirements_file):
        click.secho("No plugin specs passed", fg="red")
        return

    controller.install_plugins(requirement, requirements_file)


def main():
    """
    Primary entry point for the statey CLI
    """
    cli(prog_name="statey", auto_envvar_prefix="STATEY")


if __name__ == "__main__":
    main()
