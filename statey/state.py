"""
A statey state specifies a storage configuration for a state and provides an interface for creating graphs
"""
from contextlib import contextmanager
from typing import Any, Sequence, Dict, ContextManager, Optional

from statey.plan import Plan, PlanExecutor, ApplyResult
from statey.resource import ResourceGraph, Registry
from statey.storage import Storage, Serializer, Middleware
from statey.storage.lib.file import FileStorage
from statey.storage.lib.serializer.json import JSONSerializer


class State:
    """
	Handles communication with storage backends
	"""

    def __init__(
        self,
        predicate: Any = "state.json",
        storage: Storage = FileStorage("."),
        serializer: Serializer = JSONSerializer(),
        middlewares: Sequence[Middleware] = (),
        registry: Optional[Registry] = None
    ) -> None:
        """
		Initialize a State instance.
		`predicate` - Some identifier instructing the state backend how to store the state.
		This could be a string or SQLAlchemy clause, for example
		`storage` - StorageBackend instance
		`serializer` - Serializer instance to encode state data
		`middlewares` - Arbitrarily processing middlewares to be used to perform transformations
		on state data before it is stored. An encryption middleware could be one example.
		"""
        if registry is None:
            registry = Registry()
        self.predicate = predicate
        self.storage = storage
        self.serializer = serializer
        self.middlewares = middlewares
        self.registry = registry

    def graph(self, **kwargs: Dict[str, Any]) -> ResourceGraph:
        """
		Shortcut to create a new graph from a state directly
		"""
        return ResourceGraph(self.registry)

    def refresh(self, graph: ResourceGraph) -> ResourceGraph:  # pylint: disable=no-self-use
        """
		Given the existing state snapshot, retrieve a new, updated snapshot
		"""
        # Return a copy of the original graph instead of mutating the original
        out_graph = graph.copy()

        for node in out_graph.graph:
            data = graph.query(node, False)
            snapshot = data["snapshot"]
            resource = data["resource"]

            refreshed_snapshot = resource.refresh(snapshot)
            if refreshed_snapshot is None:
                out_graph.graph.nodes[node]["exists"] = False
                out_graph.graph.nodes[node]["snapshot"] = snapshot
            else:
                refreshed_snapshot = refreshed_snapshot.reset_factories(resource)
                out_graph.graph.nodes[node]["exists"] = True
                out_graph.graph.nodes[node]["snapshot"] = refreshed_snapshot

        return out_graph

    def apply_middlewares(self, state_data: bytes) -> bytes:
        """
		Apply any middlewares to the state data
		"""
        value = state_data
        for mid in self.middlewares:
            value = mid.apply(value)
        return value

    def unapply_middlewares(self, state_data: bytes) -> bytes:
        """
		Unapply middlewares from the state data (in reverse order)
		"""
        value = state_data
        for mid in reversed(self.middlewares):
            value = mid.unapply(value)
        return value

    def read(
        self, context: Any, refresh: bool = True, registry: Optional[Registry] = None
    ) -> Optional[ResourceGraph]:
        """
		Load the current state snapshot, refreshing if necessary

		context is output from read_context()'s __enter__ method
		"""
        if registry is None:
            registry = self.registry
        state_data = self.storage.read(self.predicate, context)
        if state_data is None:
            return None
        state_data = self.unapply_middlewares(state_data)
        current_graph = self.serializer.load(state_data, registry)
        return self.refresh(current_graph) if refresh else current_graph

    @contextmanager
    def read_context(self) -> ContextManager[None]:
        """
		Context manager for reading states
		"""
        with self.storage.read_context(self.predicate) as ctx:
            yield ctx

    def write(self, graph: ResourceGraph, context: Any) -> None:
        """
		Write the given state snapshot to remote storage
		"""
        state_data = self.serializer.dump(graph)
        state_data = self.apply_middlewares(state_data)
        self.storage.write(self.predicate, context, state_data)

    @contextmanager
    def write_context(self) -> ContextManager[None]:
        """
		Context manager for writing states
		"""
        with self.storage.write_context(self.predicate) as ctx:
            yield ctx

    def plan(
        self,
        graph: ResourceGraph,
        state_graph: Optional[ResourceGraph] = None,
        refresh: bool = True,
    ) -> Plan:
        """
        Create a plan to apply the given graph. Optionally pass a state graph as well.
        If no state graph is passed, one will be retrieved from the state using load_state(refresh=refresh).
        The refresh flag is provided as a convenience, and will be passed as the `refresh` argument
        of load_state(). If `state_graph` is provided and `refresh=True`, it will be refreshed using
        state.refresh()
        """
        from statey.plan import Plan

        if graph.registry is not self.registry:
            raise exc.ForeignGraphError(
                f"Argument `graph` contained a graph constructed using a different registry: "
                f"{graph.registry}. Expected: {self.registry}."
            )

        if state_graph is not None and state_graph.registry is not self.registry:
            raise exc.ForeignGraphError(
                f"Argument `state_graph` contained a graph constructed using a different registry: "
                f"{state_graph.registry}. Expected: {self.registry}."
            )

        with self.read_context() as ctx:
            if state_graph is None:
                state_graph = self.read(ctx, refresh=False)

            # Need to run resolve_partial once on recently read states so that factory
            # values are computed
            if state_graph is not None:
                state_graph = state_graph.resolve_all(partial=True)

            refreshed_state = state_graph
            if refresh and state_graph is not None:
                refreshed_state = self.refresh(refreshed_state)
                refreshed_state = refreshed_state.resolve_all(partial=True)

            plan = Plan(
                config_graph=graph, state_graph=refreshed_state, original_state_graph=state_graph,
            )
            plan.build()

            return plan

    def apply(self, plan: Plan, executor: Optional[PlanExecutor] = None) -> ApplyResult:
        """
        Apply the given plan with the given executor, updating the state storage accordingly
        """
        with self.write_context() as ctx:
            result = plan.apply(executor)
            state_graph = result.state_graph.resolve_all(lambda field: field.store)
            self.write(state_graph, ctx)
        return result
