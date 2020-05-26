import dataclasses as dc
import textwrap as tw
from typing import Sequence, Dict, Any, Optional

import click
import networkx as nx

from statey.executor import ExecutionInfo
from statey.plan import Plan, PlanNode
from statey.syms import utils, symbols
from statey.task import Task, SessionSwitch, ResourceGraphOperation, TaskStatus


def data_to_lines(data: Any) -> Sequence[str]:
	"""
	Render the data to a readable, yaml-like structure
	"""
	if isinstance(data, symbols.Unknown):
		return ['<unknown>']
	if isinstance(data, list):
		out_lines = []
		for item in data:
			lines = [
				tw.indent(line, '- ' if idx == 0 else '  ')
				for idx, line in enumerate(data_to_lines(item))
			]
			out_lines.extend(lines)
		return out_lines
	if isinstance(data, dict):
		out_lines = []
		for key in sorted(data):
			val = data[key]
			lines = data_to_lines(val)
			if len(lines) <= 1:
				out_lines.append(f'{key}: {"".join(lines)}')
			else:
				out_lines.append(f'{key}:')
				out_lines.extend(tw.indent(line, '  ') for line in lines)
		return out_lines

	return [str(data)]


def data_to_string(data: Any) -> str:
	"""
	Give a nice human-readable representaiton of the data using YAML
	"""
	return '\n'.join(data_to_lines(data))


def is_metatask(task: Task) -> bool:
	"""
	Indicate whether this is a "metatask" that shouldn't be displayed to the
	user in most circumstances
	"""
	return isinstance(task, (SessionSwitch, ResourceGraphOperation))


@dc.dataclass(frozen=True)
class PlanNodeSummary:
	"""
	Contains basic human-redable details about a plan node
	"""
	show_tasks: nx.DiGraph
	plan_node: PlanNode

	def _config_summary(self) -> str:
		return data_to_string(self.plan_node.config_data) if self.plan_node.config_type else '<none>'

	def _current_summary(self) -> str:
		return data_to_string(self.plan_node.current_data) if self.plan_node.current_type else '<none>'

	def data_to_string(self, column_split: int) -> str:
		current_lines = self._current_summary().split('\n')
		config_lines = self._config_summary().split('\n')

		max_n_lines = max(len(current_lines), len(config_lines))

		for lines in [current_lines, config_lines]:
			while len(lines) < max_n_lines:
				lines.append('')

		def sep(idx):
			return ' => '
			# return ' => ' if idx % 2 == 0 else '    '

		out_lines = [
			''.join([current_line.ljust(column_split), sep(idx), config_line])
			for idx, (current_line, config_line) in enumerate(zip(current_lines, config_lines))
		]
		return '\n'.join(out_lines)

	def to_string(self, column_split: int, indent: int = 2) -> str:
		if len(self.show_tasks.nodes) == 0:
			return ''

		indent_str = ' ' * indent
		data_string = self.data_to_string(column_split - indent)

		tasks_lines = [f'- {click.style("%s task(s):" % len(self.show_tasks.nodes), bold=True)}']
		for node in nx.topological_sort(self.show_tasks):
			task = self.show_tasks.nodes[node]['task']
			line = f'- {click.style(node, bold=True)}: {type(task).__name__}'
			line = tw.indent(line, indent_str)
			tasks_lines.append(line)

		tasks_string = '\n'.join(tasks_lines)

		type_lines = []
		if self.plan_node.current_type is not None:
			type_string = f'- {click.style("type (current)", bold=True)}: {self.plan_node.current_type}'
			type_lines.append(type_string)

		if self.plan_node.config_type is not None:
			type_string = f'- {click.style("type", bold=True)}: {self.plan_node.config_type}'
			type_lines.append(type_string)

		type_string = '\n'.join(type_lines)


		data_string_with_title = '\n'.join([
			f'- {click.style("data", bold=True)}:',
			tw.indent(data_string, indent_str)
		])

		data_string = tw.indent(data_string, indent_str * 2)
		lines = [
			f'- {click.style(self.plan_node.key, bold=True)}:',
			tw.indent(tasks_string, indent_str),
			tw.indent(type_string, indent_str),
			tw.indent(data_string_with_title, indent_str)
		]
		return '\n'.join(lines)


@dc.dataclass(frozen=True)
class PlanSummary:
	"""
	Contains basic human-readable details about a plan
	"""
	plan: Plan
	node_summaries: Sequence[PlanNodeSummary]

	def to_string(self, column_split: int, indent: int = 2) -> str:
		summaries = []
		for node in self.node_summaries:
			summary = node.to_string(column_split, indent)
			if summary:
				summaries.append(summary)

		if not summaries:
			return click.style('This plan is empty :)', fg='green', bold=True)
		return '\n\n'.join(summaries)


@dc.dataclass(frozen=True)
class ExecutionSummary:
	"""
	Summary info about executioninfo
	"""
	exec_info: ExecutionInfo


	def tasks_by_status(self) -> Dict[TaskStatus, Sequence[str]]:
		"""
		Return all task keys in this task graph grouped by their status
		"""
		out = {}
		for node in self.exec_info.task_graph.task_graph.nodes:
			status = self.exec_info.task_graph.get_info(node).status
			task = self.exec_info.task_graph.get_task(node)
			if is_metatask(task) and status == TaskStatus.SUCCESS:
				continue
			out.setdefault(status, []).append(node)
		return out

	def color_for_status(self, status: TaskStatus) -> Optional[str]:
		"""
		Return the color (as a string) that we want to use to print this related to this status
		"""
		color_dict = {
			TaskStatus.SUCCESS: 'green',
			TaskStatus.FAILED: 'red',
			TaskStatus.SKIPPED: 'yellow'
		}
		return color_dict.get(status)

	def to_string(self, indent: int = 2) -> str:
		"""
		Render a human-readable view describing what occurred in this
		exec info
		"""
		task_dict = self.tasks_by_status()
		duration = (self.exec_info.end_timestamp - self.exec_info.start_timestamp).total_seconds()

		run_time_line = click.style(f'Run time: {duration:.2f}s.', bold=True)

		failed_tasks = task_dict.get(TaskStatus.FAILED, [])
		skipped_tasks = task_dict.get(TaskStatus.SKIPPED, [])
		success_tasks = task_dict.get(TaskStatus.SUCCESS, [])

		if task_dict:
			task_lines = [f'Tasks completed by status:']
			for status in sorted(task_dict):
				status_name = click.style(status.name, bold=True, fg=self.color_for_status(status))
				task_lines.append(
					f'- {status_name}: {len(task_dict[status])} task(s)'
				)
			task_line = '\n'.join(task_lines)
		else:
			task_line = click.style('No tasks executed', bold=True, fg='yellow')

		is_success = not self.exec_info.cancelled_by_errors and not self.exec_info.cancelled_by
		if is_success:
			lines = [
				click.style('Plan executed successfully.', fg='green', bold=True),
				task_line,
				'',
				run_time_line
			]
			return '\n'.join(lines)

		if failed_tasks:
			lines = [
				click.style(f'Plan execution failed!', fg='red', bold=True),
				task_line,
				'',
				run_time_line
			]
			for task_name in failed_tasks:
				error = self.exec_info.task_graph.get_info(task_name).error
				name = click.style(task_name, bold=True, fg=self.color_for_status(TaskStatus.FAILED))
				lines.append(f'- {name}: {type(error).__name__}: {error}')
			return '\n'.join(lines)

		lines = [
			click.style(f'Statey exited unexpectedly during execution!', fg='yellow', bold=True),
			task_line,
			'',
			run_time_line
		]
		return '\n'.join(lines)


@dc.dataclass(frozen=True)
class Inspector:
	"""
	Utilities for visualizing plans in a human-readable manner.
	"""
	def plan_summary(self, plan: Plan) -> PlanSummary:
		nodes = []

		for node in plan.nodes:
			task_graph = node.get_task_graph(plan.state_graph, plan.config_session)

			remove = set()
			for sub_node in task_graph.nodes:
				task = task_graph.nodes[sub_node]['task']
				if is_metatask(task):
					remove.add(sub_node)

			keep = set(task_graph.nodes) - remove
			utils.subgraph_retaining_dependencies(task_graph, keep)

			summary = PlanNodeSummary(task_graph, node)
			nodes.append(summary)

		return PlanSummary(plan, nodes)

	def execution_summary(self, exec_info: ExecutionInfo) -> ExecutionSummary:
		"""
		Get a summary for an exec_info instance
		"""
		return ExecutionSummary(exec_info)


def simple_print_graph(graph: nx.DiGraph, print_func=print) -> None:
	print_func("Nodes:")
	for node in graph.nodes:
		print(f"{node}: {graph.nodes[node]}")

	print_func()
	print_func("Edges:")
	for tup in graph.edges:
		if len(tup) == 3:
			left, right, idx = tup
			print_func(f"{left} -> {right}[{idx}]: {graph.edges[left, right, idx]}")
		else:
			left, right = tup
			print_func(f"{left} -> {right}: {graph.edges[left, right]}")
