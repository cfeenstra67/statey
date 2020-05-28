import asyncio
import click
import importlib
import shutil

import statey as st
from statey.executor import AsyncIOGraphExecutor
from statey.hooks import register_default_plugins
from statey.plan import DefaultMigrator
from statey.cli.graph_utils import Inspector
from statey.cli.state_manager import FileStateManager


inspector = Inspector()


@click.group('statey')
@click.option('--state', help='File to use to save state.', default='state.json')
@click.pass_context
def cli(ctx, state):
	ctx.ensure_object(dict)
	ctx.obj['state_manager'] = FileStateManager(state)
	ctx.obj['terminal_size'] = shutil.get_terminal_size((80, 20))
	# Set up all default plugins
	register_default_plugins()


async def refresh_graph(graph):
	with click.progressbar(
		length=len(graph.graph.nodes),
		label=click.style('Refreshing state...', fg='yellow')
	) as bar:
		async for key in graph.refresh(st.registry):
			bar.update(1)


def _plan(ctx, module, session_name):
	module_obj = importlib.import_module(module)

	session = getattr(module_obj, session_name)
	resource_graph = ctx.obj['state_manager'].load(st.registry)

	loop = asyncio.get_event_loop()
	loop.run_until_complete(refresh_graph(resource_graph))

	click.echo(
		f'Loaded {click.style(session_name, fg="green", bold=True)} from '
		f'{click.style(module, fg="green", bold=True)} successfully.'
	)

	migrator = DefaultMigrator()
	plan = migrator.plan(session, resource_graph)

	click.secho(f'Planning completed successfully.', fg='green')
	click.echo()

	plan_summary = inspector.plan_summary(plan)

	summary_string = plan_summary.to_string(ctx.obj['terminal_size'].columns)

	click.echo(summary_string)

	return plan


@cli.command()
@click.option('--session-name', help='Set a module attribute other than `session` to use for planning.', default='session')
@click.argument('module')
@click.pass_context
def plan(ctx, module, session_name):
	_plan(ctx, module, session_name)


@cli.command()
@click.option('--session-name', help='Set a module attribute other than `session` to use for planning.', default='session')
@click.argument('module')
@click.pass_context
def apply(ctx, module, session_name):
	plan = _plan(ctx, module, session_name)

	# Sort of hacky way to check if the plan is empty--executing would only update the state.
	if not inspector.plan_summary(plan).non_empty_summaries(ctx.obj['terminal_size'].columns):
		return

	click.echo()
	if not click.confirm(f'Do you want to {click.style("apply", bold=True)} these changes?'):
		raise click.Abort

	executor = AsyncIOGraphExecutor()
	task_graph = plan.task_graph()

	try:
		exec_info = executor.execute(task_graph)

		exec_summary = inspector.execution_summary(exec_info)

		exec_summary_string = exec_summary.to_string()

		click.echo(exec_summary_string)
	finally:
		ctx.obj['state_manager'].dump(task_graph.resource_graph, st.registry)


if __name__ == '__main__':
	cli(prog_name='statey', auto_envvar_prefix='STATEY')
