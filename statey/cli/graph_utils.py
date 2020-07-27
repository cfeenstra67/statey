import asyncio
import dataclasses as dc
import io
import textwrap as tw
import traceback
import time
from typing import Sequence, Dict, Any, Optional, Tuple, Callable

import click
import networkx as nx
from asciidag.graph import Graph as AsciiDagGraph
from asciidag.node import Node as AsciiDagNode

import statey as st
from statey.executor import ExecutionInfo
from statey.plan import Plan, PlanNode
from statey.syms import utils, types, impl, Object
from statey.syms.path import PathParser
from statey.task import Task, SessionSwitch, ResourceGraphOperation, TaskStatus


class TaskWrapper(Task):
    """
    Base class for tasks. A task is a unit of computation
    """

    task: Task

    @property
    def description(self) -> Optional[str]:
        return self.task.description

    def always_eager(self) -> bool:
        return self.task.always_eager()


@dc.dataclass(frozen=True)
class TaskLogger(TaskWrapper):
    """
    Log events and heartbeats for tasks
    """

    task: Task
    key: str
    heartbeat_interval: float = 15.0

    async def run(self) -> None:
        start = time.time()
        asyncio_task = asyncio.ensure_future(self.task.run())

        while True:
            shielded = asyncio.shield(asyncio_task)
            try:
                await asyncio.wait_for(shielded, timeout=self.heartbeat_interval)
            except asyncio.TimeoutError:
                click.echo(
                    f"Task {click.style(self.key, fg='yellow')} still running "
                    f"({time.time() - start:.2f}s elapsed)."
                )
                continue
            except asyncio.CancelledError:
                asyncio_task.cancel()
                raise
            return


class ExecutorLoggingPlugin:
    """
	Plugin to log signals when caught
	"""

    def __init__(
        self, show_metatasks: bool = False, heartbeat_interval: float = 15.0
    ) -> None:
        self.task_starts = {}
        self.show_metatasks = show_metatasks
        self.heartbeat_interval = heartbeat_interval

    @st.hookimpl
    def caught_signal(
        self, signals: int, max_signals: int, executor: "TaskGraphExecutor"
    ) -> None:
        if signals >= max_signals:
            click.secho(
                f"Caught {signals} signals(s) >= max_signals = {max_signals}. "
                f"Exiting forcefully, this may cause data loss.",
                fg="red",
            )
        else:
            color = "yellow" if signals == 1 else "red"
            click.secho(
                f"Caught {signals} signal(s), exiting gracefully. Additional signals may cause data loss.",
                fg=color,
            )

    def should_show(self, task: Task) -> bool:
        return self.show_metatasks or not is_metatask(task)

    @st.hookimpl
    def before_run(self, key: str, task: Task, executor: "TaskGraphExecutor") -> None:
        self.task_starts[key] = time.time()
        if self.should_show(task):
            click.echo(f'Task {click.style(key, fg="yellow")} is running.')

    @st.hookimpl
    def after_run(
        self, key: str, task: Task, status: TaskStatus, executor: "TaskGraphExecutor"
    ) -> None:
        duration = time.time() - self.task_starts[key]
        color = color_for_status(status)
        styled_status = click.style(status.name, fg=color, bold=True)
        if self.should_show(task):
            click.echo(
                f'Task {click.style(key, fg="yellow")} finished after {duration:.2f}s with status {styled_status}.'
            )

    @st.hookimpl
    def task_wrapper(
        self, key: str, task: Task, executor: "TaskGraphExecutor"
    ) -> Callable[[Task], Task]:
        if not self.should_show(task):
            return None
        return lambda input_task: TaskLogger(input_task, key, self.heartbeat_interval)


def color_for_status(status: TaskStatus) -> Optional[str]:
    """
	Return the color (as a string) that we want to use to print this related to this status
	"""
    color_dict = {
        TaskStatus.SUCCESS: "green",
        TaskStatus.FAILED: "red",
        TaskStatus.SKIPPED: "yellow",
    }
    return color_dict.get(status)


def truncate_string(value: str, length: int) -> str:
    if len(value) <= length:
        return value
    return value[: length - 3] + "..."


class ColoredTypeRenderer(types.TypeStringRenderer):
    """
	Type string renderer that will add coloration
	"""

    style_map = {
        types.TypeStringToken.RIGHT_BRACE: lambda x: click.style(x, fg="yellow"),
        types.TypeStringToken.LEFT_BRACE: lambda x: click.style(x, fg="yellow"),
        types.TypeStringToken.COLON: lambda x: click.style(x, fg="yellow"),
        types.TypeStringToken.COMMA: lambda x: click.style(x, fg="yellow"),
        types.TypeStringToken.TYPE_NAME: lambda x: click.style(x, fg="cyan"),
    }
    state_style_map = {
        "UP": lambda x: click.style(x, fg="green", bold=True),
        "DOWN": lambda x: click.style(x, fg="red", bold=True),
        None: lambda x: click.style(x, fg="yellow", bold=True),
    }

    def render(self, value: str, token: types.TypeStringToken) -> str:
        if token in self.style_map:
            return self.style_map[token](value)
        return value

    def render_state(self, name: str) -> str:
        return self.state_style_map.get(name, self.state_style_map[None])(name)


def data_to_lines(data: Any, name_func=lambda x: x) -> Sequence[str]:
    """
	Render the data to a readable, yaml-like structure
	"""
    if isinstance(data, Object) and isinstance(data._impl, impl.Unknown):
        return ["<unknown>"]
    if isinstance(data, list):
        out_lines = []
        for item in data:
            lines = [
                tw.indent(line, "- " if idx == 0 else "  ")
                for idx, line in enumerate(data_to_lines(item, name_func=name_func))
            ]
            out_lines.extend(lines)
        return out_lines
    if isinstance(data, dict):
        out_lines = []
        for key in sorted(data):
            val = data[key]
            lines = data_to_lines(val, name_func=name_func)
            if len(lines) <= 1:
                out_lines.append(f'{name_func(key)}: {"".join(lines)}')
            else:
                out_lines.append(f"{name_func(key)}:")
                out_lines.extend(tw.indent(line, "  ") for line in lines)
        return out_lines

    return [repr(data)]


def data_to_string(data: Any, name_func=lambda x: x) -> str:
    """
	Give a nice human-readable representaiton of the data using YAML
	"""
    return "\n".join(data_to_lines(data, name_func=name_func))


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

    def _style_config_name(self, name: str) -> str:
        return click.style(name, bold=True)

    def _style_current_name(self, name: str) -> str:
        return click.style(name, bold=True)

    def _config_summary(self) -> str:
        if (
            not self.plan_node.config_type
            or self.plan_node.config_type == types.EmptyType
        ):
            return "<none>"
        return data_to_string(
            self.plan_node.config_data, name_func=self._style_config_name
        )

    def _current_summary(self) -> str:
        if (
            not self.plan_node.current_type
            or self.plan_node.current_type == types.EmptyType
        ):
            return "<none>"
        return data_to_string(
            self.plan_node.current_data, name_func=self._style_current_name
        )

    def data_to_string(self, max_width: int) -> str:
        if (
            self.plan_node.current_type == self.plan_node.config_type
            and self.plan_node.current_type != types.EmptyType
        ):
            differ = st.registry.get_differ(self.plan_node.current_type)
            diff = differ.diff(self.plan_node.current_data, self.plan_node.config_data)
            current_lines = []
            config_lines = []
            path_parser = PathParser()

            if not diff:
                return click.style("<no diff>", bold=True)

            for subdiff in diff:
                current_diff_lines = data_to_lines(
                    subdiff.left, name_func=self._style_current_name
                )
                path_str = path_parser.join(subdiff.path)

                if len(current_diff_lines) <= 1:
                    current_lines.append(
                        f"{self._style_current_name(path_str)}:"
                        f' {"".join(current_diff_lines)}'
                    )
                else:
                    current_lines.append(f"{self._style_current_name(path_str)}:")
                    current_lines.extend(
                        map(lambda x: tw.indent(x, "  "), current_diff_lines)
                    )

                config_diff_lines = data_to_lines(
                    subdiff.right, name_func=self._style_config_name
                )
                if len(config_diff_lines) <= 1:
                    config_lines.append(
                        f"{self._style_config_name(path_str)}:"
                        f' {"".join(config_diff_lines)}'
                    )
                else:
                    config_lines.append(f"{self._style_config_name(path_str)}:")
                    config_lines.extend(
                        map(lambda x: tw.indent(x, "  "), config_diff_lines)
                    )

                if len(config_lines) > len(current_lines):
                    current_lines.extend(
                        [click.style("", bold=True)]
                        * (len(config_lines) - len(current_lines))
                    )

                if len(current_lines) > len(config_lines):
                    config_lines.extend(
                        [click.style("", bold=True)]
                        * (len(current_lines) - len(config_lines))
                    )

        else:
            current_lines = self._current_summary().split("\n")
            config_lines = self._config_summary().split("\n")

        max_n_lines = max(len(current_lines), len(config_lines))

        for lines in [current_lines, config_lines]:
            while len(lines) < max_n_lines:
                lines.append("")

        if not current_lines:
            return ""

        max_line_length = max(map(len, current_lines))
        buffer_length = 6

        column_split = min((max_width - buffer_length) // 2, max_line_length)

        current_lines = [truncate_string(line, column_split) for line in current_lines]
        config_lines = [
            truncate_string(line, max_width - column_split - buffer_length)
            for line in config_lines
        ]

        def sep(idx):
            return click.style("  =>  ", fg="yellow", bold=True)
            # return ' => ' if idx % 2 == 0 else '    '

        out_lines = [
            "".join([current_line.ljust(column_split), sep(idx), config_line])
            for idx, (current_line, config_line) in enumerate(
                zip(current_lines, config_lines)
            )
        ]
        return "\n".join(out_lines)

    def to_string(self, max_width: int, indent: int = 2) -> str:
        if all(
            is_metatask(self.show_tasks.nodes[node]["task"])
            for node in self.show_tasks.nodes
        ):
            return ""

        style_key = lambda x: click.style(x, fg="green", bold=True)

        indent_str = " " * indent
        data_string = self.data_to_string(max_width)

        tasks_lines = [f'- {style_key("%s task(s):" % len(self.show_tasks.nodes))}']
        for node in nx.topological_sort(self.show_tasks):
            task = self.show_tasks.nodes[node]["task"]
            desc_str = ": " + task.description if task.description else ""
            line = f'- {click.style(node, fg="yellow")}{desc_str}'
            line = tw.indent(line, indent_str)
            tasks_lines.append(line)

        tasks_string = "\n".join(tasks_lines)

        type_lines = []
        renderer = ColoredTypeRenderer()
        if (
            self.plan_node.current_type is not None
            and self.plan_node.current_type != self.plan_node.config_type
        ):
            rendered = self.plan_node.current_type.render_type_string(renderer)
            type_string = f'- {style_key("type (current)")}: {rendered}'
            type_lines.append(type_string)

        state_lines = []
        if self.plan_node.current_state is not None and (
            self.plan_node.config_state is None
            or self.plan_node.current_state.state.name
            != self.plan_node.config_state.state.name
        ):
            state = renderer.render_state(self.plan_node.current_state.state.name)
            state_lines.append(f'- {style_key("state (current)")}: {state}')

        if self.plan_node.config_type is not None:
            rendered = self.plan_node.config_type.render_type_string(renderer)
            type_string = f'- {style_key("type")}: {rendered}'
            type_lines.append(type_string)

        if self.plan_node.config_state is not None:
            state = renderer.render_state(self.plan_node.config_state.state.name)
            state_lines.append(f'- {style_key("state")}: {state}')

        type_string = "\n".join(type_lines)
        state_string = "\n".join(state_lines)

        data_string_with_title = "\n".join(
            [f'- {style_key("data")}:', tw.indent(data_string, indent_str * 2)]
        )

        resource = (
            self.plan_node.config_state.resource
            if self.plan_node.config_state
            else self.plan_node.current_state.resource
        )

        data_string = tw.indent(data_string, indent_str * 2)
        lines = [
            f'- {style_key(self.plan_node.key)}{click.style("[", fg="yellow")}'
            f'{click.style(resource, fg="cyan")}{click.style("]", fg="yellow")}:',
            tw.indent(tasks_string, indent_str),
            tw.indent(type_string, indent_str),
            tw.indent(state_string, indent_str),
            tw.indent(data_string_with_title, indent_str),
        ]
        return "\n".join(lines)


def ascii_dag(graph: nx.DiGraph, key: Callable[[str], str] = lambda x: x) -> str:
    """
    Render the given DAG as a string, showing dependencies
    """
    outfile = io.StringIO()
    ascii_graph = AsciiDagGraph(outfile)

    nodes = {}
    tips = []

    for node in reversed(list(nx.topological_sort(graph))):
        parent_nodes = [nodes[path] for path in graph.succ[node]]
        ascii_node = nodes[node] = AsciiDagNode(key(node), parents=parent_nodes)
        if not graph.pred[node]:
            tips.append(ascii_node)

    ascii_graph.show_nodes(tips)
    return outfile.getvalue()


@dc.dataclass(frozen=True)
class PlanSummary:
    """
	Contains basic human-readable details about a plan
	"""

    plan: Plan
    node_summaries: Sequence[PlanNodeSummary]
    show_metatasks: bool

    def non_empty_summaries(
        self, max_width: int, indent: int = 2
    ) -> Sequence[PlanNodeSummary]:
        summaries = []
        for node in self.node_summaries:
            summary = node.to_string(max_width, indent)
            if summary:
                summaries.append(summary)
        return summaries

    def to_string(self, max_width: int, indent: int = 2) -> str:
        summaries = self.non_empty_summaries(max_width, indent)
        if not summaries:
            return click.style("This plan is empty :)", fg="green", bold=True)
        return "\n\n".join(summaries)

    def task_dag_string(self) -> str:
        task_graph = self.plan.task_graph().task_graph
        keep = set()

        for node in task_graph:
            task = task_graph.nodes[node]["task"]
            if self.show_metatasks or not is_metatask(task):
                keep.add(node)

        utils.subgraph_retaining_dependencies(task_graph, keep)
        return ascii_dag(graph=task_graph, key=lambda x: click.style(x, fg="yellow"))


@dc.dataclass(frozen=True)
class ExecutionSummary:
    """
	Summary info about executioninfo
	"""

    exec_info: ExecutionInfo
    show_metatasks: bool

    def tasks_by_status(self) -> Dict[TaskStatus, Sequence[str]]:
        """
		Return all task keys in this task graph grouped by their status
		"""
        out = {}
        for node in self.exec_info.task_graph.task_graph.nodes:
            status = self.exec_info.task_graph.get_info(node).status
            task = self.exec_info.task_graph.get_task(node)
            if (
                not self.show_metatasks
                and is_metatask(task)
                and status == TaskStatus.SUCCESS
            ):
                continue
            out.setdefault(status, []).append(node)
        return out

    def to_string(self, indent: int = 2) -> str:
        """
		Render a human-readable view describing what occurred in this
		exec info
		"""
        task_dict = self.tasks_by_status()
        duration = (
            self.exec_info.end_timestamp - self.exec_info.start_timestamp
        ).total_seconds()

        run_time_line = click.style(f"Run time: {duration:.2f}s.", bold=True)

        failed_tasks = task_dict.get(TaskStatus.FAILED, [])
        skipped_tasks = task_dict.get(TaskStatus.SKIPPED, [])
        success_tasks = task_dict.get(TaskStatus.SUCCESS, [])

        if task_dict:
            task_lines = [f"Tasks completed by status:"]
            for status in sorted(task_dict, key=lambda x: x.value):
                status_name = click.style(
                    status.name, bold=True, fg=color_for_status(status)
                )
                task_lines.append(f"- {status_name}: {len(task_dict[status])} task(s)")
            task_line = "\n".join(task_lines)
        else:
            task_line = click.style("No tasks executed", bold=True, fg="yellow")

        is_success = (
            not self.exec_info.cancelled_by_errors and not self.exec_info.cancelled_by
        )
        if is_success:
            lines = [
                click.style("Plan executed successfully.", fg="green", bold=True),
                task_line,
                "",
                run_time_line,
            ]
            return "\n".join(lines)

        if failed_tasks:
            lines = [
                click.style(f"Plan execution failed!", fg="red", bold=True),
                task_line,
                "",
                run_time_line,
            ]
            for task_name in failed_tasks:
                error = self.exec_info.task_graph.get_info(task_name).error
                name = click.style(
                    task_name, bold=True, fg=color_for_status(TaskStatus.FAILED)
                )
                lines.append(f"- {name}:\n{error.format_exception()}\n")
            return "\n".join(lines)

        lines = [
            click.style(
                f"Statey exited unexpectedly during execution!", fg="yellow", bold=True
            ),
            task_line,
            "",
            run_time_line,
        ]
        return "\n".join(lines)


@dc.dataclass(frozen=True)
class Inspector:
    """
	Utilities for visualizing plans in a human-readable manner.
	"""

    def plan_summary(self, plan: Plan, show_metatasks: bool = False) -> PlanSummary:
        nodes = []

        for node in plan.nodes:
            task_graph = node.get_task_graph(plan.state_graph, plan.config_session)

            remove = set()
            for sub_node in task_graph.nodes:
                task = task_graph.nodes[sub_node]["task"]
                if not show_metatasks and is_metatask(task):
                    remove.add(sub_node)

            keep = set(task_graph.nodes) - remove
            utils.subgraph_retaining_dependencies(task_graph, keep)

            summary = PlanNodeSummary(task_graph, node)
            nodes.append(summary)

        return PlanSummary(plan, nodes, show_metatasks)

    def execution_summary(
        self, exec_info: ExecutionInfo, show_metatasks: bool = False
    ) -> ExecutionSummary:
        """
		Get a summary for an exec_info instance
		"""
        return ExecutionSummary(exec_info, show_metatasks)


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
