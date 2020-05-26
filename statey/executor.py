import abc
import asyncio
import dataclasses as dc
import enum
import logging
from typing import Sequence, Coroutine, Dict, Optional, Any

import networkx as nx
from networkx.algorithms.dag import descendants

from statey.resource import ResourceSession, ResourceGraph
from statey.syms import utils, session, exc
from statey.task import Task, TaskStatus, TaskInfo, SessionSwitch


logger = logging.getLogger(__name__)


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

	def set_status(self, key: str, status: TaskStatus, error: Optional[Exception] = None, skipped_by: Optional[str] = None) -> None:
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

	# def finalize_output_sessions(self) -> None:
	# 	"""
	# 	Clean up the state of any output sessions, adding unknowns where
	# 	relevant.
	# 	"""
	# 	# Collect sources with at least one non-completed task
	# 	non_completed = {
	# 		self.task_graph.nodes[node]['source'] for node in self.task_graph.nodes
	# 		# This includes NOT_STARTED, PENDING, and SKIPPED
	# 		if self.get_info(node).status < TaskStatus.FAILED
	# 	}

	# 	# for each one of these, set the state in the output session based on
	# 	# a checkpoint, the previous state, or a null state as a backup
	# 	for node in non_completed:
	# 		try:
	# 			checkpoint = self.checkpoint_session.resource_state(node)
	# 		except exc.SymbolKeyError:
	# 			pass
	# 		else:
	# 			self.output_session.ns.new(node, checkpoint.resource_state.state.type, overwrite=True)
	# 			self.output_session.set_data(node, checkpoint.data)
	# 			continue





	# 		else:




















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


class TaskGraphExecutor(abc.ABC):
	"""
	Executes all the tasks in a given graph
	"""
	@abc.abstractmethod
	def execute(
		self,
		task_graph: TaskGraph,
		strategy: ExecutionStrategy = ExecutionStrategy.TENTATIVE,
		max_signals: int = 2
	) -> ExecutionInfo:
		"""

		"""
		raise NotImplementedError


class AsyncIOGraphExecutor(TaskGraphExecutor):
	"""
	Graph executors that uses python asyncio couroutines for concurrency
	"""
	async def after_task_success(self, key: str, exec_info: ExecutionInfo) -> None:
		"""
		Callback after a task completes _successfully_
		"""
		exec_info.task_graph.set_status(key, TaskStatus.SUCCESS)

		# If the run has been cancelled and we're doing tentative execution, stop here.
		if exec_info.cancelled_by is not None and exec_info.strategy == ExecutionStrategy.TENTATIVE:
			for child_key in exec_info.task_graph.get_descendants(key):
				if exec_info.task_graph.get_info(child_key).status != TaskStatus.SKIPPED:
					exec_info.task_graph.set_status(child_key, TaskStatus.SKIPPED, skipped_by=exec_info.cancelled_by)
			return

		# If we get a signal to cancel, we'll stop creating new tasks regardless of the execution
		# strategy.
		if exec_info.cancelled_by_errors:
			for child_key in exec_info.task_graph.get_descendants(key):
				if exec_info.task_graph.get_info(child_key).status != TaskStatus.SKIPPED:
					exec_info.task_graph.set_status(child_key, TaskStatus.SKIPPED, skipped_by='ERROR')
			return

		# Otherwise, schehdule any ready tasks
		coros = [
			self.task_wrapper(child_key, exec_info.task_graph.get_task(child_key), exec_info)
			for child_key in exec_info.task_graph.get_ready_tasks()
		]
		if coros:
			await asyncio.wait(coros)

	async def after_task_failure(self, key: str, exec_info: ExecutionInfo, error: Exception) -> None:
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

		asyncio_task = asyncio.ensure_future(task.run())
		exec_info.tasks[key] = asyncio_task
		exec_info.task_graph.set_status(key, TaskStatus.PENDING)
		try:
			await asyncio_task
		except Exception as err:
			await self.after_task_failure(key, exec_info, err)
		else:
			await self.after_task_success(key, exec_info)

	def hard_cancel(self, exec_info: ExecutionInfo) -> None:
		"""
		Cancel any running tasks in exec_info
		"""
		for task in exec_info.tasks.values():
			if not task.done():
				task.cancel()

	async def task_loop(self, coro: Coroutine, max_signals: int, exec_info: ExecutionInfo) -> None:
		"""
		Handle signals like KeyboardInterrupt and SystemExit properly, only cancelling running
		tasks when we reach max_signals.
		"""
		signals = 0

		while True:
			try:
				return await coro
			except Exception:
				raise
			# Catch anything else (e.g. SystemExit, KeyboardInterrupt) and handle it
			# gracefully, avoiding cancelling running tasks as long as the user will
			# let us.
			except BaseException as err:
				signals += 1
				exec_info.cancelled_by_errors.append(err)

				if signals >= max_signals:
					logger.info(
						"Caught %s. Cancelling tasks and reraising as signals >= "
						"max_signals (%s >= %s)",
						err,
						signals,
						max_signals,
					)
					self.hard_cancel(exec_info)
					raise

				logger.info(
					"Caught %s. Continuing as signals < max_signals (%s < %s)."
					" Please wait for the program to exit normally, as data loss may"
					"occur otherwise",
					err,
					signals,
					max_signals,
				)

	async def execute_async(
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

		await self.task_loop(asyncio.wait(ready_wrappers), max_signals, exec_info)

		return exec_info

	def execute(
		self,
		task_graph: TaskGraph,
		strategy: ExecutionStrategy = ExecutionStrategy.TENTATIVE,
		max_signals: int = 2
	) -> ExecutionInfo:

		coro = self.execute_async(task_graph, strategy, max_signals)
		loop = asyncio.get_event_loop()
		return loop.run_until_complete(coro)
