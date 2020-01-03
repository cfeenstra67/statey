"""
Execution code for plans
"""
import abc
import concurrent.futures
import enum
import queue
import threading
from functools import partial

from statey.exc import JobAborted
from statey.schema import SchemaSnapshot
from .change import Change


class ExecState(enum.Enum):
    """
    Indicates the state of a particular plan execution instance
    """

    NOT_STARTED = "not_started"
    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"


# pylint: disable=too-many-instance-attributes
class PlanJob:
    """
    A PlanJob contains state information about the progress of a pending plan execution
    """

    def __init__(self) -> None:
        self.queue = queue.Queue()
        self.complete = []
        self.errors = []
        self._aborted = False
        self._aborted_lock = threading.Lock()
        self._callback_lock = threading.RLock()

        self.state = ExecState.NOT_STARTED
        self.error_callback = lambda key, change, error: None
        self.complete_callback = lambda key, change, snapshot: None
        self.change_hook = lambda x: x
        self.result_hook = lambda x: x

    def add_change(self, key: str, change: Change) -> None:
        """
        Add the given change to the queue
        """
        self.queue.put((key, self.handle_change(key, change)))
        self.state = ExecState.PENDING

    def apply_change(self, change: Change) -> SchemaSnapshot:
        """
        Apply the given change and return the result, passing to result_hook if relevant
        """
        if self.aborted:
            raise JobAborted
        result = change.apply()
        return self.result_hook(result)

    def handle_future(self, key: str, change: Change, future: concurrent.futures.Future) -> None:
        """
        Handle an execution future properly, passing to the appropriate callback based on the result.
        """
        error = future.exception()
        if error is not None:
            self.change_error(key, change, error)
        else:
            self.change_complete(key, change, future.result())

    # pylint: disable=unused-argument
    def handle_change(self, key: str, change: Change) -> Change:
        """
        Apply change_hook to the change if relevant
        """
        return self.change_hook(change)

    def change_complete(self, key: str, change: Change, snapshot: SchemaSnapshot) -> None:
        """
        Handler for when a change is applied successfully
        """
        self.queue.task_done()
        with self._callback_lock:
            if not change.null:
                self.complete.append((key, change, snapshot))
            self.complete_callback(key, change, snapshot)

    def change_error(self, key: str, change: Change, error: Exception) -> None:
        """
        Handler for when we encounter an error when applying a plan.
        """
        self.queue.task_done()
        with self._callback_lock:
            self.errors.append((key, change, error))
            self.error_callback(key, change, error)

    def abort(self) -> None:
        """
        Abort the current job. This means no more changes will be processed, and the
        executor should return once all pending tasks are complete.
        """
        with self._aborted_lock:
            self._aborted = True
        self.state = ExecState.ERROR

    @property
    def aborted(self) -> bool:
        """
        Property indicating whether a job has been aborted.
        """
        with self._aborted_lock:
            return self._aborted

    def join(self) -> None:
        """
        'join' the job queue, returning when all pending tasks are complete.
        """
        self.queue.join()

        # Acquire the callback lock to ensure callbacks are finished
        with self._callback_lock:
            pass

        if self.state == ExecState.PENDING:
            self.state = ExecState.SUCCESS


# pylint: disable=too-few-public-methods
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

    def __init__(self, num_threads: int = 8, poll_interval: float = 0.1) -> None:
        self.num_threads = num_threads
        self.poll_interval = poll_interval

    def execute(self, job: PlanJob) -> PlanJob:

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:

            futures = []
            while not job.aborted:
                try:
                    key, change = job.queue.get(timeout=self.poll_interval)
                except queue.Empty:
                    break

                future = executor.submit(job.apply_change, change)
                futures.append(future)
                future.add_done_callback(partial(job.handle_future, key, change))
                future.add_done_callback(lambda x: futures.remove(future))

            # Wait for all pending tasks to complete
            concurrent.futures.wait(futures)

            job.join()
            return job
