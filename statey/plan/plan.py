"""
A Plan object contains the core logic for generating and applying operation plans to statey states
"""
from typing import Sequence, Optional, Dict, Any

import dataclasses as dc
import networkx as nx

from statey import exc
from statey.schema import Symbol, SchemaSnapshot, Reference
from statey.resource import ResourceGraph
from .change import Change, NoChange, Create, Delete, DeleteAndRecreate, Update
from .executor import PlanExecutor, PlanJob, DefaultPlanExecutor


# pylint: disable=too-few-public-methods
@dc.dataclass(frozen=True, eq=True)
class ApplyResult:
    """
    Simple helper object containing information about a completed job run
    """

    unprocessed: Sequence[Change]
    job: PlanJob
    state_graph: ResourceGraph


# pylint: disable=too-many-instance-attributes
class PlanKernel:
    """
    Several callbacks that work together with PlanJob to execute plans
    """

    def __init__(
        self,
        change_graph: nx.DiGraph,
        state_graph: ResourceGraph,
        existing_state_graph: Optional[ResourceGraph] = None,
    ) -> None:
        self.change_graph = change_graph
        self.state_graph = state_graph
        self.existing_state_graph = existing_state_graph
        self.job = None
        self.ancestor_map = {
            key: list(self.change_graph.predecessors(key)) for key in self.change_graph
        }
        self.successor_map = {
            key: list(self.change_graph.successors(key)) for key in self.change_graph
        }
        self.completes = set()
        self.errors = set()

    def bind(self, job: PlanJob) -> None:
        """
        Bind this kernel to the given job, setting all callbacks to the correct values
        """
        job.complete_callback = self.complete_callback
        job.error_callback = self.error_callback
        job.change_hook = self.change_hook
        job.result_hook = self.result_hook
        self.job = job

        # Add top-level items to the queue
        for path, ancestors in self.ancestor_map.items():
            if len(ancestors) > 0:
                continue
            change = self.change_graph.nodes[path]["change"]
            job.add_change(path, change)

    # pylint: disable=unused-argument
    def complete_callback(self, path: str, change: Change, snapshot: SchemaSnapshot) -> None:
        """
        Callback to bind to a job as job.complete_callback
        """
        self.completes.add(path)

        data = self.state_graph.query(path, False)
        data["snapshot"] = snapshot
        data["exists"] = True

        for successor_path in self.successor_map[path]:
            ancestors = self.ancestor_map[successor_path]

            # This shouldn't happen, because the job should already be aborted.
            # Just here to be safe
            if set(ancestors) & set(self.errors):
                self.job.abort()
                return

            # There are still unfinished ancestors, so don't queue this
            # item yet
            if set(ancestors) - set(self.completes):
                continue

            # We should get here exactly once for each path (across all calls of complete())
            self.job.add_change(successor_path, self.change_graph.nodes[successor_path]["change"])

    # pylint: disable=unused-argument
    def error_callback(self, path: str, change: Change, error: Exception) -> None:
        """
        Callback to bind to a job as job.error_callback
        """
        self.errors.add(path)

        self.state_graph.query(path, False)["error"] = error

        if self.existing_state_graph is not None and path in self.existing_state_graph.graph.nodes:
            data = self.state_graph.query(path, False)
            existing_data = self.existing_state_graph.query(path, False)
            data["snapshot"] = existing_data["snapshot"]
            data["exists"] = existing_data["exists"]
        else:
            data = self.state_graph.query(path, False)
            data["snapshot"] = None
            data["exists"] = False

        self.job.abort()

    # pylint: disable=unused-argument
    def change_hook(self, path: str, change: Change) -> Change:
        """
        Hook to transform a change before it's applied
        """
        change.snapshot = change.snapshot.resolve(
            self.state_graph, lambda field: not field.computed
        )
        return change

    def result_hook(self, result: SchemaSnapshot) -> Change:
        """
        Hook to transform a snapshot resulting from an applied change
        """
        # Deletions return None
        if result is None:
            return result
        return result.resolve(self.state_graph, lambda field: field.store)


class Plan:
    """
    A statey plan contains all information required to apply any quantity
    of changes to a resource graph
    """

    def __init__(
        self,
        config_graph: ResourceGraph,
        state_graph: Optional[ResourceGraph] = None,  # Possibly refereshed
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

                if isinstance(new_value, Symbol) or old_value != new_value:
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

    def process_delete_node(self, path: str, successors: Sequence[str]) -> None:
        """
        Process a given path for deletion. Note that all successors of the given
        node must already exist in the graph
        """
        node = self.state_graph.query(path, False)
        snapshot = node["snapshot"]
        resource = node["resource"]

        self.graph.add_node(path, change=Delete(resource, None, snapshot))
        for successor in successors:
            self.graph.add_edge(successor, path)

    def add_deletions(self) -> None:
        """
        Add any Delete changes, with dependencies.

        All non-Delete nodes should already be added to the graph before this is called
        """
        if self.state_graph is None:
            return

        not_processed = set(self.state_graph.graph) - set(self.config_graph.graph)
        successor_map = {
            key: list(self.state_graph.graph.successors(key)) for key in self.state_graph.graph
        }
        max_depth = -1

        while len(not_processed) > 0:
            max_depth += 1

            for path in sorted(not_processed):
                successors = successor_map[path]

                if set(successors) & not_processed:
                    continue

                self.process_delete_node(path, successors)
                not_processed.remove(path)

        self.max_depth = max(self.max_depth, max_depth)

    def validate(self) -> None:
        """
        Validate that the current plan makes sense i.e. no circular references
        """
        ancestor_map = {key: list(self.graph.predecessors(key)) for key in self.graph}
        for path, ancestors in ancestor_map.items():
            for ancestor in ancestors:
                ancestor_ancestors = ancestor_map[ancestor]
                if path in ancestor_ancestors:
                    raise exc.GraphIntegrityError(
                        f"Circular reference detected in plan between resources"
                        f' "{path}" and "{ancestor}".'
                    )

    def build(self) -> None:
        """
        Build a directed graph of changes based on resource dependencies
        """
        not_processed = set(self.config_graph.graph)
        ancestor_map = {
            key: list(self.config_graph.graph.predecessors(key)) for key in self.config_graph.graph
        }
        max_depth = -1

        while len(not_processed) > 0:
            max_depth += 1

            for path in sorted(not_processed):
                ancestors = ancestor_map[path]

                if set(ancestors) & not_processed:
                    continue

                self.process_node(path, ancestors)
                not_processed.remove(path)

        self.max_depth = max(self.max_depth, max_depth)
        self.add_deletions()
        self.validate()

    def apply(self, executor: Optional[PlanExecutor] = None) -> ApplyResult:
        """
        Execute the current plan as a job using the given executor
        """
        if executor is None:
            executor = DefaultPlanExecutor()

        job = PlanJob()

        if all(node["change"].null for node in self.graph.nodes.values()):
            return ApplyResult(
                state_graph=self.state_graph, job=job, unprocessed=sorted(self.config_graph.graph),
            )

        state_graph = self.config_graph.copy()
        kernel = PlanKernel(
            state_graph=state_graph,
            change_graph=self.graph,
            existing_state_graph=self.original_state_graph,
        )
        kernel.bind(job)

        job = executor.execute(job)

        processed = set(path for path, _, _ in job.complete + job.errors)
        not_processed = set(self.graph) - processed

        # Fill prior states for any unprocessed resources
        for path in not_processed:
            if self.state_graph is None or path not in self.state_graph.graph:
                continue

            if (
                self.original_state_graph is not None
                and path in self.original_state_graph.graph
                and self.original_state_graph.graph.nodes[path]["snapshot"] is not None
                and self.original_state_graph.graph.nodes[path].get("exists", False)
            ):
                state_graph.graph.nodes[path]["snapshot"] = self.original_state_graph.graph.nodes[
                    path
                ]["snapshot"]
                state_graph.graph.nodes[path]["exists"] = self.original_state_graph.graph.nodes[
                    path
                ].get("exists", False)

        return ApplyResult(
            state_graph=state_graph,
            job=job,
            unprocessed=[(path, self.graph.nodes[path]["change"]) for path in not_processed],
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
                ancestors = sorted(self.graph.predecessors(path))
                if set(ancestors) & not_processed:
                    continue

                change = self.graph.nodes[path]["change"]
                if change.null:
                    skipped.add(path)
                    not_processed.remove(path)
                    continue

                fields = {}

                for name, new_value in change.snapshot.items():
                    old_value = change.old_snapshot and change.old_snapshot[name]
                    item = fields[name] = {
                        "old": old_value,
                        "new": new_value,
                        "change": new_value != old_value,
                    }
                    for key, val in item.items():
                        if isinstance(val, Symbol):
                            item[key] = "<computed>"
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
