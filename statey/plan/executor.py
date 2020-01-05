"""
Execution code for plans
"""
import asyncio
import logging
from typing import Type

from statey import exc
from .task_graph import TaskGraph


LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods
class AsyncGraphExecutor:
    """
    Executes all of the changes in a plan. Provided to the Plan.execute() method
    """

    def __init__(self, task_graph_cls: Type[TaskGraph] = TaskGraph) -> None:
        self.task_graph_cls = task_graph_cls

    @staticmethod
    def validate_graph(graph: TaskGraph) -> None:
        """
        Validate graph before returning to make sure that everything was processed
        properly.
        """
        for node in graph.graph:
            data = graph.graph.nodes[node]
            if "task" not in data and not data.get("skipped", False):
                raise exc.TaskLost(node, data["coro"], tuple(graph.graph.pred[node]))

            if not data["task"].done():
                raise exc.TaskStillRunning(node, data["task"])

    async def task_loop(self, task: asyncio.Task, graph: TaskGraph, max_signals: int) -> None:
        """
        While loop to await the given task and catch system exceptions gracefully
        """
        signals = 0
        while True:
            try:
                return await task
            # Raise normal exceptions
            except Exception:
                raise
            # Catch anything else (e.g. SystemExit, KeyboardInterrupt) and handle it gracefully,
            # avoiding cancelling running tasks as long as the user will let us.
            except BaseException as err:  # pylint: disable=broad-except
                signals += 1
                if signals >= max_signals:
                    LOGGER.info(
                        "Caught %s. Cancelling tasks and reraising as signals >= max_signals (%s >= %s)",
                        err,
                        signals,
                        max_signals,
                    )
                    # Hard operation, cancels anything running
                    graph.cancel(force=True)
                    raise
                LOGGER.info(
                    "Caught %s. Continuing as signals < max_signals (%s < %s)."
                    " Please wait for the program to exit normally, as data loss may"
                    "occur otherwise",
                    err,
                    signals,
                    max_signals,
                )
                # Soft operation, leaves running tasks alone
                graph.cancel()

    async def execute(self, graph: TaskGraph, max_signals: int = 1) -> TaskGraph:
        """
        Execute the given job
        """
        tasks = graph.schedule(asyncio.ensure_future)
        parent_task = asyncio.ensure_future(asyncio.wait(tasks))
        await self.task_loop(parent_task, graph, max_signals)
        self.validate_graph(graph)
        return graph
