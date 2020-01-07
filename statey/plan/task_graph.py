"""
A task graph is a generalized way to execute of tasks related by edges in a graph.
"""
import asyncio
import itertools
import logging
from functools import partial
from typing import Callable, Coroutine, Tuple, Optional, Hashable, Set

import networkx as nx

from statey import exc
from statey.resource import ResourceGraph
from statey.schema import SchemaSnapshot
from statey.utils.helpers import detect_circular_references


ChangeCoro = Coroutine[None, None, Optional[SchemaSnapshot]]


LOGGER = logging.getLogger(__name__)


class TaskGraph:
    """
    A task graph executes a graph of coroutines. This implementation will not schedule
    any child tasks once an error is encountered, but it will still process other
    tasks until all directed paths either end successfully or in error.
    """

    def __init__(
        self,
        graph: nx.DiGraph,
        error_callback: Callable[[Hashable, Exception], None] = lambda key, err: None,
        success_callback: Callable[
            [Hashable, Optional[SchemaSnapshot]], None
        ] = lambda key, snap: None,
    ) -> None:
        """
        `graph` should contain a graph of coroutines.
        """
        self.graph = graph.copy()
        self.error_callback = error_callback
        self.success_callback = success_callback

        self.scheduled = set()
        self.done = set()
        self.cancelled = False
        detect_circular_references(self.graph)

    def ready_to_schedule(self, parent: Optional[Hashable] = None) -> Set[Hashable]:
        """
        Return a tuple of keys of coroutines that are ready to schedule.
        """
        candidates = set(self.graph) if parent is None else set(self.graph.succ[parent])

        paths = []
        for node in candidates - (self.scheduled | self.done):
            if set(self.graph.pred[node]) - self.done:
                continue
            paths.append(node)

        return set(paths)

    def cancel(self, force: bool = True) -> None:
        """
        Cancel execution of this task graph. This means no new tasks will be launched,
        but existing tasks will continue running unless force=True.
        """
        self.cancelled = True
        for path in self.graph:
            node = self.graph.nodes[path]
            task = node.get("task")
            if task is None:
                node["skipped"] = True
                node["skipped_from"] = set()
            elif force and not task.done():
                task.cancel()

    async def wait(self) -> None:
        """
        Wait for all pending tasks to complete
        """
        tasks = [
            self.graph.nodes[path]["task"]
            for path in self.graph
            if "task" in self.graph.nodes[path]
        ]
        await asyncio.gather(*tasks)

    def mark_coros_skipped(
        self, parent: Hashable, ignore: Set[Hashable] = frozenset()
    ) -> None:
        """
        Mark any coroutines dependent on `parent` as skipped.
        """
        ignore = set(ignore) if isinstance(ignore, frozenset) else ignore
        for other_path in self.graph.successors(parent):
            if other_path in ignore:
                continue
            self.graph.nodes[other_path]["skipped"] = True
            self.graph.nodes[other_path].setdefault("skipped_from", set()).add(parent)
            ignore.add(other_path)
            self.mark_coros_skipped(other_path, ignore)

    async def coro_wrapper(
        self,
        task_factory: Callable[[ChangeCoro], asyncio.Task],
        path: Hashable,
        coro: ChangeCoro,
    ) -> Callable[[], Coroutine[None, None, None]]:
        """
        Wrap a couroutine including graph traversal logic
        """
        LOGGER.debug("Awaiting task for %s.", path)
        try:
            result = await coro
        except Exception as exc:
            LOGGER.exception("Error in task for %s", path)
            self.error_callback(path, exc)
            self.mark_coros_skipped(path)
            # Reraise the error so we capture it in the task object
            raise

        LOGGER.debug("Completed task for %s.", path)
        self.done.add(path)
        self.success_callback(path, result)

        if self.cancelled:
            self.mark_coros_skipped(path)
            return result

        paths = self.ready_to_schedule(path)
        LOGGER.debug(
            "Scheduling %d task(s) after completing %s. (%r)", len(paths), path, paths
        )
        if len(paths) > 0:
            tasks = map(partial(self.schedule, task_factory), paths)
            coros = itertools.chain.from_iterable(tasks)
            coros = list(coros)
            LOGGER.debug(
                "Scheduled %d task(s) after completing %s. (%s)",
                len(paths),
                path,
                paths,
            )
            await asyncio.wait(coros)
            LOGGER.debug(
                "Completed %d task(s) after completing %s. (%s)",
                len(paths),
                path,
                paths,
            )

        return result

    def schedule(
        self,
        task_factory: Callable[[ChangeCoro], asyncio.Task],
        key: Optional[Hashable] = None,
    ) -> Tuple[asyncio.Task, ...]:
        """
        Schedule a coroutine for execution
        """
        if key is None:
            schedule_path = partial(self.schedule, task_factory)
            tasks = map(schedule_path, self.ready_to_schedule())
            return tuple(itertools.chain.from_iterable(tasks))

        node = self.graph.nodes[key]
        if "task" in node:
            raise exc.TaskAlreadyScheduled(key, node["task"])

        wrapper = self.coro_wrapper(task_factory, key, node["coro"])
        task = node["task"] = task_factory(wrapper)
        self.scheduled.add(key)
        return (task,)


class PlanGraph(TaskGraph):
    """
    A TaskGraph specifically for applying plans, including resolving logic
    """

    @staticmethod
    def build_coro_graph(change_graph: nx.DiGraph) -> nx.DiGraph:
        """
        Given a change graph, build a coroutine graph containing all non-null
        change coroutines
        """
        coro_graph = nx.DiGraph()
        for path in change_graph:
            node = change_graph.nodes[path]
            if node["change"].null:
                continue
            coro_graph.add_node(
                path, coro=node["change"].apply(), change=node["change"]
            )

        for src, dest in change_graph.edges:
            if src not in coro_graph or dest not in coro_graph:
                continue
            coro_graph.add_edge(src, dest)

        return coro_graph

    def __init__(
        self,
        change_graph: nx.DiGraph,
        state_graph: ResourceGraph,
        existing_state_graph: Optional[ResourceGraph] = None,
    ) -> None:
        coro_graph = self.build_coro_graph(change_graph)
        super().__init__(
            graph=coro_graph,
            error_callback=self.error_cb,
            success_callback=self.success_cb,
        )
        self.change_graph = change_graph
        self.state_graph = state_graph
        self.existing_state_graph = existing_state_graph

    def finalize_state(self) -> None:
        """
        Intended to be called after all scheduled tasks have completed. Fill any
        unprocessed tasks or nulls in the state graph with existing states, if
        they exist. Otherwise, remove those nodes from the graph (this sould always
        be possible because we process each resource's dependencies before the
        resource itself).
        """
        # First process unexecuted changes
        new_edges = []
        new_nodes = []

        for path in self.change_graph:
            # Skip if it's already in the task graph and has a completed task
            if (
                path in self.graph
                and self.graph.nodes[path].get("task") is not None
                and self.graph.nodes[path]["task"].exception() is None
            ):
                continue

            # Currently this is always using the prior state instead of the
            # refreshed one, but in theory we should always be able to refresh from
            # the old state so this is be OK: A refreshed state exists only if an
            # initial one does, so we'll never lose any data. If the execution goes
            # successfully only NoChange snapshots should get here as well, and
            # those should always be functionally equal to the refreshed version.
            if (
                self.existing_state_graph is not None
                and path in self.existing_state_graph.graph
            ):
                existing_node = self.existing_state_graph.query(path, False)
                if path in self.state_graph.graph:
                    node = self.state_graph.query(path, False)
                    node["snapshot"] = existing_node["snapshot"]
                    node["exists"] = existing_node["exists"]
                else:
                    new_nodes.append((path, existing_node))
                    for node, edges in self.existing_state_graph.graph.pred[
                        path
                    ].items():
                        for edge in edges.values():
                            new_edges.append((node, path, edge))

            # if no existing state exists or the path is not in the existing state,
            # remove it from the graph
            elif path in self.state_graph.graph:
                self.state_graph.graph.remove_node(path)

        self.state_graph.graph.add_nodes_from(new_nodes)
        self.state_graph.graph.add_edges_from(new_edges)

    async def coro_wrapper(
        self,
        task_factory: Callable[[ChangeCoro], asyncio.Task],
        path: Hashable,
        coro: ChangeCoro,
    ) -> Callable[[], Coroutine[None, None, None]]:
        async def wrapper():
            change = self.graph.nodes[path]["change"]
            if change.snapshot is not None:
                change.snapshot = change.snapshot.resolve(
                    self.state_graph, lambda field: not field.computed
                )

            result = await coro
            return result

        return await super().coro_wrapper(task_factory, path, wrapper())

    def error_cb(self, path: Hashable, error: Exception) -> None:
        """
        Callback implementation for errors
        """
        node = self.state_graph.query(path, False)
        node["error"] = error

        if (
            self.existing_state_graph is not None
            and path in self.existing_state_graph.graph
        ):
            existing = self.existing_state_graph.query(path, False)
            node["snapshot"] = existing["snapshot"]
            node["exists"] = existing["exists"]
        else:
            node["snapshot"] = None
            node["exists"] = False

    def success_cb(self, path: Hashable, result: Optional[SchemaSnapshot]) -> None:
        """
        Callback implementation for successes
        """
        if result is not None:
            node = self.state_graph.query(path, False)
            node["snapshot"] = result
            node["exists"] = True
