import click
import importlib

import statey as st
from statey.executor import AsyncIOGraphExecutor
from statey.hooks import register_default_plugins
from statey.plan import DefaultMigrator
from statey.cli.graph_utils import Inspector
from statey.cli.state_manager import FileStateManager


@click.group('statey')
@click.option('--state', help='File to use to save state.', default='state.json')
@click.pass_context
def cli(ctx, state):
	ctx.ensure_object(dict)
	ctx.obj['state_manager'] = FileStateManager(state)
	# Set up all default plugins
	register_default_plugins()


@cli.command()
@click.option('--session-name', help='Set a module attribute other than `session` to use for planning.', default='session')
@click.argument('module')
@click.pass_context
def plan(ctx, module, session_name):
	module_obj = importlib.import_module(module)

	session = getattr(module_obj, session_name)
	resource_graph = ctx.obj['state_manager'].load(st.registry)

	click.echo(
		f'Loaded {click.style(session_name, fg="green", bold=True)} from '
		f'{click.style(module, fg="green", bold=True)} successfully.'
	)

	migrator = DefaultMigrator()
	plan = migrator.plan(session, resource_graph)

	click.secho(f'Planning completed successfully.', fg='green')

	inspector = Inspector()

	plan_summary = inspector.plan_summary(plan)

	# Arbitrary
	column_width = 40
	summary_string = plan_summary.to_string(column_width)

	click.echo(summary_string)


@cli.command()
@click.option('--session-name', help='Set a module attribute other than `session` to use for planning.', default='session')
@click.argument('module')
@click.pass_context
def apply(ctx, module, session_name):
	module_obj = importlib.import_module(module)

	session = getattr(module_obj, session_name)
	resource_graph = ctx.obj['state_manager'].load(st.registry)

	click.echo(
		f'Loaded {click.style(session_name, fg="green", bold=True)} from '
		f'{click.style(module, fg="green", bold=True)} successfully.'
	)

	migrator = DefaultMigrator()
	plan = migrator.plan(session, resource_graph)

	click.secho(f'Planning completed successfully.', fg='green')

	inspector = Inspector()

	plan_summary = inspector.plan_summary(plan)

	# Arbitrary
	column_width = 40
	summary_string = plan_summary.to_string(column_width)

	click.echo(summary_string)

	if not click.confirm('Do you want to confinue?'):
		raise click.Abort

	executor = AsyncIOGraphExecutor()
	task_graph = plan.task_graph()
	exec_info = executor.execute(task_graph)

	exec_summary = inspector.execution_summary(exec_info)

	exec_summary_string = exec_summary.to_string()

	click.echo(exec_summary_string)

	ctx.obj['state_manager'].dump(task_graph.resource_graph, st.registry)


if __name__ == '__main__':
	cli(prog_name='statey', auto_envvar_prefix='STATEY')
