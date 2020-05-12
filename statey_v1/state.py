"""
A statey state specifies a storage configuration for a state and provides an interface
for creating graphs
"""
import asyncio
from typing import Any, Sequence, AsyncContextManager, Optional, Type

from statey import exc
from statey.plan import Plan, AsyncGraphExecutor, ApplyResult
from statey.resource import ResourceGraph, Registry
from statey.storage import Storage, Serializer, Middleware
from statey.storage.lib.file import FileStorage
from statey.storage.lib.serializer.json import JSONSerializer
from statey.utils.helpers import asynccontextmanager


class State:
    """
	Handles communication with storage backends
	"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        predicate: Any = "state.json",
        storage: Storage = FileStorage("."),
        serializer: Serializer = JSONSerializer(),
        middlewares: Sequence[Middleware] = (),
        registry: Optional[Registry] = None,
        graph_class: Type[ResourceGraph] = ResourceGraph,
    ) -> None:
        """
		Initialize a State instance.
		`predicate` - Some identifier instructing the state backend how to store the state.
		This could be a string or SQLAlchemy clause, for example
		`storage` - StorageBackend instance
		`serializer` - Serializer instance to encode state data
		`middlewares` - Arbitrarily processing middlewares to be used to perform
        transformations
		on state data before it is stored. An encryption middleware could be one example.
		"""
        if registry is None:
            registry = Registry()
        self.predicate = predicate
        self.storage = storage
        self.serializer = serializer
        self.middlewares = middlewares
        self.registry = registry
        self.graph_class = graph_class

    def graph(self) -> ResourceGraph:
        """
		Shortcut to create a new graph from a state directly
		"""
        return self.graph_class(self.registry)

    async def refresh(
        self, graph: ResourceGraph
    ) -> ResourceGraph:  # pylint: disable=no-self-use
        """
		Given the existing state snapshot, retrieve a new, updated snapshot
		"""
        # Return a copy of the original graph instead of mutating the original
        out_graph = graph.copy()

        async def coro(node, resource, snapshot):
            refreshed_snapshot = await resource.refresh(snapshot)
            if refreshed_snapshot is None:
                out_graph.graph.nodes[node]["exists"] = False
                out_graph.graph.nodes[node]["snapshot"] = None
            else:
                refreshed_snapshot = refreshed_snapshot.reset_factories(resource)
                out_graph.graph.nodes[node]["exists"] = True
                out_graph.graph.nodes[node]["snapshot"] = refreshed_snapshot

        coros = []
        for node in out_graph.graph:
            data = graph.query(node, False)
            coros.append(coro(node, data["resource"], data["snapshot"]))

        if len(coros) > 0:
            await asyncio.wait(coros)
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

    async def read(
        self, context: Any, refresh: bool = True, registry: Optional[Registry] = None
    ) -> Optional[ResourceGraph]:
        """
		Load the current state snapshot, refreshing if necessary

		context is output from read_context()'s __enter__ method
		"""
        if registry is None:
            registry = self.registry
        state_data = await self.storage.read(self.predicate, context)
        if state_data is None:
            return None
        state_data = self.unapply_middlewares(state_data)
        current_graph = self.serializer.load(
            state_data, registry, graph_class=self.graph_class
        )
        return await self.refresh(current_graph) if refresh else current_graph

    @asynccontextmanager
    async def read_context(self) -> AsyncContextManager[None]:
        """
		Context manager for reading states
		"""
        async with self.storage.read_context(self.predicate) as ctx:
            yield ctx

    async def write(self, graph: ResourceGraph, context: Any) -> None:
        """
		Write the given state snapshot to remote storage
		"""
        state_data = self.serializer.dump(graph)
        state_data = self.apply_middlewares(state_data)
        await self.storage.write(self.predicate, context, state_data)

    @asynccontextmanager
    async def write_context(self) -> AsyncContextManager[None]:
        """
		Context manager for writing states
		"""
        async with self.storage.write_context(self.predicate) as ctx:
            yield ctx

    async def plan(
        self,
        graph: ResourceGraph,
        state_graph: Optional[ResourceGraph] = None,
        refresh: bool = True,
    ) -> Plan:
        """
        Create a plan to apply the given graph. Optionally pass a state graph as well.
        If no state graph is passed, one will be retrieved from the state using
        load_state(refresh=refresh). The refresh flag is provided as a convenience, and
        will be passed as the `refresh` argument of load_state(). If `state_graph` is
        provided and `refresh=True`, it will be refreshed using state.refresh()
        """
        if graph.registry is not self.registry:
            raise exc.ForeignGraphError(
                f"Argument `graph` contained a graph constructed using a different "
                f"registry: {graph.registry}. Expected: {self.registry}."
            )

        if state_graph is not None and state_graph.registry is not self.registry:
            raise exc.ForeignGraphError(
                f"Argument `state_graph` contained a graph constructed using a "
                f"different registry: {state_graph.registry}. Expected: "
                f"{self.registry}."
            )

        async with self.read_context() as ctx:  # pylint: disable=not-async-context-manager
            if state_graph is None:
                state_graph = await self.read(ctx, refresh=False)

            # Need to run resolve_partial once on recently read states so that factory
            # values are computed
            if state_graph is not None:
                state_graph = state_graph.resolve_all(partial=True)

            refreshed_state = state_graph
            if refresh and state_graph is not None:
                refreshed_state = await self.refresh(refreshed_state)
                refreshed_state = refreshed_state.resolve_all(partial=True)

            plan = Plan(
                config_graph=graph,
                state_graph=refreshed_state,
                original_state_graph=state_graph,
            )
            plan.build()

            return plan

    async def apply(
        self,
        plan: Plan,
        executor: Optional[AsyncGraphExecutor] = None,
        write_state: bool = True,
    ) -> ApplyResult:
        """
        Apply the given plan with the given executor, updating the state storage
        accordingly
        """
        # pylint: disable=not-async-context-manager
        async with self.write_context() as ctx:
            result = await plan.apply(executor)
            state_graph = result.state_graph.resolve_all(lambda field: field.store)
            if write_state:
                await self.write(state_graph, ctx)
        return result
