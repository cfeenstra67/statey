"""
A Plan object contains the core logic for generating and applying operation plans to statey states
"""
from operator import itemgetter
from typing import Sequence, Optional, Dict, Any

import dataclasses as dc
import networkx as nx

from statey import exc
from statey.schema import Symbol, Reference
from statey.resource import ResourceGraph
from statey.utils.helpers import truncate_string, detect_circular_references
from .change import NoChange, Create, Delete, DeleteAndRecreate, Update
from .executor import AsyncGraphExecutor
from .task_graph import PlanGraph


# pylint: disable=too-few-public-methods
@dc.dataclass(frozen=True, eq=True)
class ApplyResult:
    """
    Simple helper object containing information about a completed job run
    """

    state_graph: ResourceGraph
    complete: Sequence[Dict[str, Any]]
    error: Sequence[Dict[str, Any]]
    unprocessed: nx.DiGraph
    nulls: Sequence[Dict[str, Any]]
    success: bool


# pylint: disable=too-few-public-methods
class ComputedValue:
    """
    Simple object used in printed plans to indicate a value is computed
    """

    def __str__(self) -> str:
        return "<computed>"


class Plan:
    """
    A statey plan contains all information required to apply any quantity
    of changes to a resource graph
    """

    def __init__(
        self,
        config_graph: ResourceGraph,
        state_graph: Optional[ResourceGraph] = None,  # Possibly refreshed
        original_state_graph: Optional[ResourceGraph] = None,
    ) -> None:
        """
        `config_graph` - The ResourceGraph with the desired configuration for this
        """
        if state_graph is None and original_state_graph is not None:
            state_graph = original_state_graph
        if original_state_graph is None:
            original_state_graph = state_graph

        self.config_graph = config_graph
        self.resolve_graph = self.config_graph.copy()
        self.state_graph = state_graph
        self.original_state_graph = original_state_graph
        self.graph = nx.DiGraph(plan=self)

        self.max_depth = -1

    # pylint: disable=too-many-locals
    def process_node(self, path: str, ancestors: Sequence[str]) -> None:
        """
        Process the given node. Note that all ancestors of the given node must
        already exist in the graph
        """
        config_node = self.config_graph.query(path, False)

        # Do this operation twice so that self references resolve correctly
        snapshot = self.resolve_graph.graph.nodes[path]["snapshot"] = config_node[
            "snapshot"
        ].resolve_partial(self.resolve_graph)
        config = config_node["snapshot"]
        resource = config_node["resource"]

        if resource is None:
            raise exc.PlanError(
                f'Found a null resource configuration for snapshot at path: "{path}".'
            )

        if (
            self.state_graph is None
            or path not in self.state_graph.graph
            or not self.state_graph.query(path, False).get("exists", False)
        ):
            change = Create(resource, snapshot, None)
        else:
            old_snapshot = self.state_graph.query(resource)

            changes = {}
            recreate = False

            for key, new_value in snapshot.items():
                old_value = old_snapshot[key]

                # This is a reference to itself. This should always be the case
                # for computed fields
                if (
                    isinstance(new_value, Reference)
                    # pylint: disable=protected-access
                    and self.config_graph._path(new_value.resource) == path
                    and new_value.field.name == key
                    and not isinstance(old_value, Symbol)
                ):
                    continue

                if (
                    isinstance(new_value, Symbol)
                    or isinstance(old_value, Symbol)
                    or old_value != new_value
                ):
                    field = resource.Schema.__fields__[key]
                    if field.store:
                        changes[key] = old_value, new_value
                    if field.create_new:
                        recreate = True

            if len(changes) == 0 and not recreate:
                change = NoChange(resource, old_snapshot, old_snapshot)
            elif recreate:
                change = DeleteAndRecreate(resource, snapshot, old_snapshot)
            else:
                change = Update(resource, snapshot, old_snapshot, changes)

        self.graph.add_node(path, change=change)
        for ancestor in ancestors:
            self.graph.add_edge(ancestor, path)
        self.resolve_graph.graph.nodes[path]["snapshot"] = change.snapshot

    def process_delete_node(self, path: str, children: Sequence[str]) -> None:
        """
        Process a given path for deletion. Note that all children of the given
        node must already exist in the graph
        """
        node = self.state_graph.query(path, False)
        snapshot = node["snapshot"]
        resource = node["resource"]

        if snapshot is None:
            self.graph.add_node(path, change=NoChange(resource, None, None))
        else:
            self.graph.add_node(path, change=Delete(resource, None, snapshot))

        for child in children:
            self.graph.add_edge(child, path)

    def add_deletions(self) -> None:
        """
        Add any Delete changes, with dependencies.

        All non-Delete nodes should already be added to the graph before this is called
        """
        if self.state_graph is None:
            return

        not_processed = set(self.state_graph.graph) - set(self.config_graph.graph)
        max_depth = -1

        while len(not_processed) > 0:
            max_depth += 1

            for path in sorted(not_processed):
                children = set(self.state_graph.graph[path])

                if set(children) & not_processed:
                    continue

                self.process_delete_node(path, children)
                not_processed.remove(path)

        self.max_depth = max(self.max_depth, max_depth)

    def build(self) -> None:
        """
        Build a directed graph of changes based on resource dependencies
        """
        not_processed = set(self.config_graph.graph)
        max_depth = -1

        while len(not_processed) > 0:
            max_depth += 1

            for path in sorted(not_processed):
                ancestors = self.config_graph.graph.pred[path]

                if set(ancestors) & not_processed:
                    continue

                self.process_node(path, ancestors)
                not_processed.remove(path)

        self.max_depth = max(self.max_depth, max_depth)
        self.add_deletions()
        detect_circular_references(self.graph)

    async def apply(self, executor: Optional[AsyncGraphExecutor] = None) -> ApplyResult:
        """
        Execute the current plan as a job using the given executor
        """
        if executor is None:
            executor = AsyncGraphExecutor()

        state_graph = self.config_graph.copy()
        graph = PlanGraph(
            change_graph=self.graph,
            state_graph=state_graph,
            existing_state_graph=self.original_state_graph,
        )
        null = sorted(set(self.graph) - set(graph.graph))
        await executor.execute(graph)
        # Clean up state after execution
        graph.finalize_state()

        complete = []
        error = []
        unprocessed = nx.DiGraph()
        for path in graph.graph:
            node = graph.graph.nodes[path]

            if node.get("skipped", False):
                skipped_from = sorted(node.get("skipped_from", []))
                unprocessed.add_node(
                    path,
                    change=self.graph.nodes[path]["change"],
                    state=state_graph.graph.nodes.get(path),
                    skipped_from=skipped_from,
                )
                for src in skipped_from:
                    unprocessed.add_edge(src, path)
                continue

            task = node["task"]
            exception = task.exception()
            if exception is not None:
                error.append(
                    {
                        "path": path,
                        "change": self.graph.nodes[path]["change"],
                        "error": exception,
                        "state": state_graph.graph.nodes.get(path),
                    }
                )
                continue

            complete.append(
                {
                    "path": path,
                    "change": self.graph.nodes[path]["change"],
                    "state": state_graph.graph.nodes.get(path),
                }
            )

        complete.sort(key=itemgetter("path"))
        error.sort(key=itemgetter("path"))

        success = len(error) + len(unprocessed) == 0

        return ApplyResult(
            state_graph=state_graph,
            complete=complete,
            error=error,
            unprocessed=unprocessed,
            nulls=null,
            success=success,
        )

    def pretty_print(self) -> Dict[str, Any]:
        """
        Return a human-reabable (and serializable) version of this plan.
        Currently cannot be deserialized, just for reading
        """
        items = []
        not_processed = set(self.graph)
        skipped = set()

        while len(not_processed) > 0:
            for path in sorted(not_processed):
                ancestors = sorted(self.graph.reverse(copy=False)[path])

                if set(ancestors) & not_processed:
                    continue

                change = self.graph.nodes[path]["change"]
                if change.null:
                    skipped.add(path)
                    not_processed.remove(path)
                    continue

                fields = {}

                snapshot = change.snapshot
                # Set all nulls for deletions
                if snapshot is None:
                    kws = {k: None for k in change.resource.Schema.__fields__}
                    snapshot = change.resource.schema_helper.snapshot_cls(**kws)

                for name, new_value in snapshot.items():
                    old_value = change.old_snapshot and change.old_snapshot[name]
                    changed = (
                        isinstance(new_value, Symbol)
                        or isinstance(old_value, Symbol)
                        or old_value != new_value
                    )
                    recreate = False
                    if changed:
                        field = snapshot.source_schema.__fields__[name]
                        recreate = field.create_new

                    item = fields[name] = {
                        "old": truncate_string(old_value)
                        if isinstance(old_value, str)
                        else old_value,
                        "new": truncate_string(new_value)
                        if isinstance(new_value, str)
                        else new_value,
                        "change": changed,
                        "recreate": recreate,
                    }
                    for key, val in item.items():
                        if isinstance(val, Symbol):
                            item[key] = ComputedValue()
                    fields[name] = item

                items.append(
                    {
                        "type": type(change).__name__,
                        "path": path,
                        "after": sorted(set(ancestors) - skipped),
                        "fields": fields,
                    }
                )
                not_processed.remove(path)

        return items
