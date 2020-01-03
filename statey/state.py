"""
A statey state specifies a storage configuration for a state and provides an interface for creating sessions
"""
from contextlib import contextmanager
from typing import Any, Sequence, Dict, ContextManager, Optional

from statey.resource import ResourceGraph
from statey.session import Session
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
        self.predicate = predicate
        self.storage = storage
        self.serializer = serializer
        self.middlewares = middlewares

    def session(self, **kwargs: Dict[str, Any]) -> Session:
        """
		Retrieve a new session for this state
		"""
        return Session(self, **kwargs)

    def graph(self, **kwargs: Dict[str, Any]) -> ResourceGraph:
        """
		Shortcut to create a new session and graph from a state directly
		"""
        return self.session(**kwargs).graph()

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
        self, session: "Session", context: Any, refresh: bool = True
    ) -> Optional[ResourceGraph]:
        """
		Load the current state snapshot, refreshing if necessary

		context is output from read_context()'s __enter__ method
		"""
        state_data = self.storage.read(self.predicate, context)
        if state_data is None:
            return None
        state_data = self.unapply_middlewares(state_data)
        current_graph = self.serializer.load(state_data, session)
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
