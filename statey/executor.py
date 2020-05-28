import abc
import asyncio
import dataclasses as dc
import enum
from datetime import datetime
from typing import Sequence, Coroutine, Dict, Optional, Any

import networkx as nx
import pluggy
from networkx.algorithms.dag import descendants

from statey.hooks import hookspec, create_plugin_manager
from statey.resource import ResourceSession, ResourceGraph
from statey.syms import utils, session, exc
from statey.task import Task, TaskStatus, TaskInfo, SessionSwitch, ErrorInfo


@dc.dataclass(frozen=True)
class TaskGraph:
	"""
	Wrapper class for a flat task graph, offering API methods to manipulate it, usually with
	an executor
	"""
	task_graph: nx.DiGraph
	output_session: session.Session
	resource_graph: ResourceGraph

	def __post_init__(self) -> None:
		"""
		Fill initial states for all tasks
		"""
		for node in self.task_graph:
			self.task_graph.nodes[node]['info'] = TaskInfo(TaskStatus.NOT_STARTED)

	def set_status(self, key: str, status: TaskStatus, error: Optional[ErrorInfo] = None, skipped_by: Optional[str] = None) -> None:
		"""
		Set the status of a given task
		"""
		self.task_graph.nodes[key]['info'] = TaskInfo(status, error=error, skipped_by=skipped_by)

	def get_task(self, key: str) -> Task:
		"""
		Get the task with the given key
		"""
		return self.task_graph.nodes[key]['task']

	def get_info(self, key: str) -> TaskInfo:
		"""
		Get the task with the given key
		"""
		return self.task_graph.nodes[key]['info']

	def get_descendants(self, key: str) -> Sequence[str]:
		"""
		Get all ancestor tasks of `key`
		"""
		return descendants(self.task_graph, key)

	def get_ready_tasks(self) -> Sequence[str]:
		"""
		Get the tasks that are ready to schedule
		"""
		success_tasks = {
			node for node in self.task_graph.nodes
			if self.get_info(node).status == TaskStatus.SUCCESS
		}
		not_started_tasks = {
			node for node in self.task_graph.nodes
			if self.get_info(node).status == TaskStatus.NOT_STARTED
		}
		out = []
		for task_key in not_started_tasks:
			# If all predecessors are not "SUCCESS" we can't schedule yet
			if set(self.task_graph.pred[task_key]) - success_tasks:
				continue
			out.append(task_key)
		return out


class ExecutionStrategy(enum.Enum):
	"""
	Define different methods of execution a graph.
	EAGER means that we'll keep executing as long as we don't have a failure in a given
	branch.
	TENTATIVE means once we have one failure, we won't schedule any more tasks
	"""
	EAGER = 'eager'
	TENTATIVE = 'tentative'


@dc.dataclass
class ExecutionInfo(utils.Cloneable):
	"""
	Store some metadata about the result of an execution.
	"""
	task_graph: TaskGraph
	strategy: ExecutionStrategy
	tasks: Dict[str, asyncio.Task] = dc.field(default_factory=dict)
	cancelled_by: Optional[str] = None
	cancelled_by_errors: Sequence[BaseException] = dc.field(default_factory=list)
	start_timestamp: datetime = dc.field(default_factory=datetime.utcnow)
	end_timestamp: datetime = dc.field(default=None)


class TaskGraphHooks:
	"""
	Specifies hooks to call during task graph execution
	"""
	@hookspec
	def before_run(self, key: str, task: Task, executor: 'TaskGraphExecutor') -> None:
		"""
		Register side effects before a given task is run
		"""

	@hookspec
	def after_run(self, key: str, task: Task, status: TaskStatus, executor: 'TaskGraphExecutor') -> None:
		"""
		Register side effects after a given task is run
		"""

	@hookspec
	def caught_signal(self, signals: int, max_signals: int, executor: 'TaskGraphExecutor') -> None:
		"""
		Register side effects when a signal is caught, indicating whether the program
		is going to exit
		"""


def create_executor_plugin_manager() -> pluggy.PluginManager:
	"""
	Factory function for a plugin manager for a GraphExecutor
	"""
	pm = create_plugin_manager()
	pm.add_hookspecs(TaskGraphHooks)
	return pm


class TaskGraphExecutor(abc.ABC):
	"""
	Executes all the tasks in a given graph
	"""
	pm: pluggy.PluginManager

	@abc.abstractmethod
	def execute(
		self,
		task_graph: TaskGraph,
		strategy: ExecutionStrategy = ExecutionStrategy.TENTATIVE,
		max_signals: int = 2
	) -> ExecutionInfo:
		"""
		Execute the tasks in the given graph, returning information about the run.
		"""
		raise NotImplementedError


class AsyncIOGraphExecutor(TaskGraphExecutor):
	"""
	Graph executors that uses python asyncio couroutines for concurrency
	"""
	def __init__(self, pm: Optional[pluggy.PluginManager] = None) -> None:
		if pm is None:
			pm = create_executor_plugin_manager()
		self.pm = pm

	async def after_task_success(self, key: str, exec_info: ExecutionInfo) -> None:
		"""
		Callback after a task completes _successfully_
		"""
		exec_info.task_graph.set_status(key, TaskStatus.SUCCESS)

		ready_tasks = exec_info.task_graph.get_ready_tasks()
		always_eager_coros = []
		other_coros = []
		for task in ready_tasks:
			coro = self.task_wrapper(task, exec_info.task_graph.get_task(task), exec_info)
			if exec_info.task_graph.get_task(task).always_eager():
				always_eager_coros.append(coro)
			else:
				other_coros.append(coro)

		# If there are any ready always eager tasks, we'll execute them right away no matter what
		if always_eager_coros:
			await asyncio.wait(always_eager_coros)

		# If the run has been cancelled and we're doing tentative execution, skip any unrun descendent
		# tasks
		if exec_info.cancelled_by is not None and exec_info.strategy == ExecutionStrategy.TENTATIVE:
			for child_key in exec_info.task_graph.get_descendants(key):
				if exec_info.task_graph.get_info(child_key).status == TaskStatus.NOT_STARTED:
					exec_info.task_graph.set_status(child_key, TaskStatus.SKIPPED, skipped_by=exec_info.cancelled_by)
			return

		# If we get a signal to cancel, we'll stop creating new tasks regardless of the execution
		# strategy other than always eager tasks.
		if exec_info.cancelled_by_errors:
			for child_key in exec_info.task_graph.get_descendants(key):
				if exec_info.task_graph.get_info(child_key).status == TaskStatus.NOT_STARTED:
					exec_info.task_graph.set_status(child_key, TaskStatus.SKIPPED, skipped_by='ERROR')
			return

		# Otherwise, schehdule any non-always-eager tasks
		if other_coros:
			await asyncio.wait(other_coros)

	async def after_task_failure(self, key: str, exec_info: ExecutionInfo, error: ErrorInfo) -> None:
		"""
		Callback after a task fails
		"""
		exec_info.task_graph.set_status(key, TaskStatus.FAILED, error=error)

		for task_key in exec_info.task_graph.get_descendants(key):
			exec_info.task_graph.set_status(task_key, TaskStatus.SKIPPED, skipped_by=key)
		if exec_info.cancelled_by is None:
			exec_info.cancelled_by = key

	async def task_wrapper(self, key: str, task: Task, exec_info: ExecutionInfo) -> None:
		"""
		Wrap a coroutine to catch exceptions and set the appropriate result
		"""
		# This task may have been scheduled multiple times--if so, just exit here
		if exec_info.task_graph.get_info(key).status != TaskStatus.NOT_STARTED:
			return

		self.pm.hook.before_run(key=key, task=task, executor=self)

		asyncio_task = asyncio.ensure_future(task.run())
		exec_info.tasks[key] = asyncio_task
		exec_info.task_graph.set_status(key, TaskStatus.PENDING)
		try:
			await asyncio_task
		except Exception:
			self.pm.hook.after_run(key=key, task=task, status=TaskStatus.FAILED, executor=self)
			await self.after_task_failure(key, exec_info, ErrorInfo.exc_info())
		else:
			self.pm.hook.after_run(key=key, task=task, status=TaskStatus.SUCCESS, executor=self)
			await self.after_task_success(key, exec_info)

	def hard_cancel(self, exec_info: ExecutionInfo) -> None:
		"""
		Cancel any running tasks in exec_info
		"""
		for task in exec_info.tasks.values():
			if not task.done():
				task.cancel()

	def task_loop(self, coro: Coroutine, max_signals: int, exec_info: ExecutionInfo) -> Any:
		"""
		Handle signals like KeyboardInterrupt and SystemExit properly, only cancelling running
		tasks when we reach max_signals.
		"""
		signals = 0
		loop = asyncio.get_event_loop()

		while True:
			try:
				return loop.run_until_complete(coro)
			except Exception:
				raise
			# Catch anything else (e.g. SystemExit, KeyboardInterrupt) and handle it
			# gracefully, avoiding cancelling running tasks as long as the user will
			# let us.
			except BaseException as err:
				signals += 1
				exec_info.cancelled_by_errors.append(err)

				will_cancel = signals >= max_signals

				self.pm.hook.caught_signal(signals=signals, max_signals=max_signals, executor=self)

				if will_cancel:
					self.hard_cancel(exec_info)
					raise

	def execute(
		self,
		task_graph: TaskGraph,
		strategy: ExecutionStrategy = ExecutionStrategy.TENTATIVE,
		max_signals: int = 2
	) -> ExecutionInfo:
		
		ready_tasks = {
			key: task_graph.get_task(key)
			for key in task_graph.get_ready_tasks()
		}
		exec_info = ExecutionInfo(task_graph, strategy)
		ready_wrappers = [self.task_wrapper(key, task, exec_info) for key, task in ready_tasks.items()]

		if not ready_wrappers:
			return exec_info

		self.task_loop(asyncio.wait(ready_wrappers), max_signals, exec_info)

		exec_info.end_timestamp = datetime.utcnow()
		return exec_info
