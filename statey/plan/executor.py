import abc
import concurrent.futures
import enum
import queue
import threading
import time
from ctypes import c_bool
from typing import Type, Optional

from .change import Change
from statey.exc import JobAborted
from statey.schema import SchemaSnapshot


class ExecState(enum.Enum):
	"""
	Indicates the state of a particular plan execution instance
	"""
	NOT_STARTED = 'not_started'
	PENDING = 'pending'
	SUCCESS = 'success'
	ERROR = 'error'


class PlanJob:

	def __init__(self) -> None:
		self.queue = queue.Queue()
		self.complete = []
		self.errors = []
		self._aborted = False
		self._aborted_lock = threading.Lock()
		self._callback_lock = threading.RLock()

		self.state = ExecState.NOT_STARTED
		self.error_callback = None
		self.complete_callback = None
		self.change_hook = None

	def add_change(self, key: str, change: Change) -> None:
		self.queue.put((key, change))
		self.state = ExecState.PENDING

	def handle_change(self, key: str, change: Change) -> Change:
		if self.change_hook is None:
			return change
		return self.change_hook(key, change)

	def change_complete(self, key: str, change: Change) -> None:
		if self.complete_callback is None:
			return
		with self._callback_lock:
			self.complete.append((key, change))
			self.complete_callback(key, change)

	def change_error(self, key: str, change: Change, error: Exception) -> None:
		self.queue.task_done()
		if self.error_callback is None:
			return
		with self._callback_lock:
			self.errors.append((key, change, error))
			self.error_callback(key, change)

	def abort(self) -> None:
		with self._aborted_lock:
			self._aborted = True
		self.state = ExecState.ERROR

	@property
	def aborted(self) -> bool:
		with self._aborted_lock:
			return self._aborted

	def join(self) -> None:
		"""
		'join' the job queu(s)
		"""
		if hasattr(self.queue, 'join'):
			self.queue.join()
		elif hasattr(self.queue, 'join_thread'):
			self.queue.join_thread()

		if self.state == ExecState.PENDING and self.queue.empty():
			self.state = ExecState.SUCCESS


def _execute_job(job: PlanJob, key: str, change: Change) -> Optional[SchemaSnapshot]:
	"""
	Helper function to have a serializable version of a change runner (pickle)
	"""
	try:
		if job.aborted:
			raise JobAborted
		snapshot = job.handle_change(key, change).apply()
	except Exception as exc:
		job.change_error(key, change, exc)
	else:
		job.change_complete(key, change, snapshot)


class PlanExecutor(abc.ABC):
	"""
	Executes all of the changes in a plan. Provided to the Plan.execute() method
	"""
	@abc.abstractmethod
	def execute(self, job: PlanJob) -> PlanJob:
		"""
		Execute the given job
		"""
		raise NotImplementedError


class DefaultPlanExecutor(PlanExecutor):
	"""
	Execute a plan using a ThreadPoolExecutor
	"""
	def __init__(
			self,
			num_threads: int = 8,
			poll_interval: float = .1
	) -> None:
		"""
		Default 
		"""
		self.num_threads = num_threads
		self.poll_interval = poll_interval

	def execute(self, job: PlanJob) -> PlanJob:

		with concurrent.futures.ThreadPoolExecutor(max_threads=self.num_threads) as executor:

			futures = []
			while not job.aborted:
				try:
					key, change = job.queue.get(timeout=self.poll_interval)
				except queue.Empty:
					break

				future = None
				def _done():
					futures.remove(future)

				future = executor.submit(_execute_job, job, key, change)
				future.add_done_callback(_done)
				futures.append(future)

			# Wait for all pending tasks to complete
			concurrent.futures.wait(futures)

			job.join()
			return job
