"""
A ResouceGraph is one of the core data structures in statey. It provides an interface for
building and operating on resource graphs
"""
from typing import Optional, Union, Type, Callable

import networkx as nx

from statey import exc
from statey.schema import Field, SchemaSnapshot, Symbol, QueryRef
from .resource import Resource


class ResourceGraph:
    """
	A ResourceGraph provides public methods to build a NetworkX graph
	object that is used for operations like plan(), apply(), etc.
	"""

    def __init__(self, session: "Session") -> None:
        """
		`session` - a statey session
		"""
        self.session = session
        self.graph = self.graph_factory()

    def graph_factory(self) -> nx.Graph:
        """
		Create a nx.Graph instance for this ResourceGraph
		"""
        return nx.MultiDiGraph(resource_graph=self)

    def query(self, resource: QueryRef, snapshot_only: bool = True) -> SchemaSnapshot:
        """
		Query a specific field value from the graph
		"""
        node = self.graph.nodes[self._path(resource)]
        if snapshot_only:
            return node["snapshot"]
        return node

    def copy(self) -> "ResourceGraph":
        """
		Return a copy of this resource graph
		"""
        graph = type(self)(self.session)
        graph.graph = self.graph.copy()
        return graph

    def _path(self, resource: QueryRef) -> str:
        return self.session.path(resource) if isinstance(resource, Resource) else resource

    # pylint: disable=too-many-arguments
    def _add_at_path(
        self,
        path: str,
        snapshot: SchemaSnapshot,
        exists: Optional[bool],
        resource: Optional[Resource],
        resource_cls: Type[Resource],
    ) -> None:
        """
		Add the given snapshot at the given path. This operation is atomic, so either
		it succeeds entirely or fails entirely
		"""
        if path in self.graph:
            raise exc.GraphIntegrityError(
                f'Attempting to insert snapshot into the graph for path "{path}",'
                f" but a snapshot already exists at this path."
            )

        if snapshot.source_schema is not resource_cls.Schema:
            raise exc.GraphIntegrityError(
                f'Attempting to insert snapshot into the graph for path "{path}",'
                f" but the snapshot's schema does not match resource_cls.Schema "
                f"(got {repr(snapshot.source_schema)}, expected {resource_cls.Schema})."
            )

        if resource is not None and not isinstance(resource, resource_cls):
            raise exc.GraphIntegrityError(
                f'Attempt to insert snapshot into the graph for path "{path}",'
                f" but the snapshot's resource is not of type resource_cls "
                f"(got {repr(type(resource))}, expected subclass of {repr(resource_cls)}."
            )

        new_graph = self.graph.copy()
        kws = {"snapshot": snapshot, "resource_cls": resource_cls, "resource": resource}
        if exists is not None:
            kws["exists"] = exists
        new_graph.add_node(path, **kws)

        for name, value in snapshot.items():
            # Only need special behavior for symbols, otherwise
            # no additional analysis required
            if not isinstance(value, Symbol):
                continue

            for other_resource, field in value.refs():
                resource_path = self._path(other_resource)
                # Ignore self references
                if resource_path == path:
                    continue
                data = self.query(resource_path, False)
                other_resource_cls = data["resource_cls"]

                if field not in other_resource_cls.Schema.__fields__:
                    raise exc.InvalidReference(
                        f"Field {repr(field)} does not belong to schema {repr(other_resource_cls.Schema)}"
                        f" of resource class {repr(other_resource_cls)}."
                    )

                if resource_path not in new_graph:
                    raise exc.InvalidReference(
                        f"Failed to add snapshot {repr(snapshot)} at path {path}. The field {repr(field)}"
                        f" references a path not yet added to this graph: {resource_path}."
                    )

                new_graph.add_edge(
                    resource_path, path, symbol=value, destination_field_name=name, field=field,
                )

        # Only update self.graph if we get through the add operation successfully
        self.graph = new_graph

    # pylint: disable=too-many-arguments
    def add(
        self,
        snapshot: Union[Resource, SchemaSnapshot],
        name: Optional[str] = None,
        exists: Optional[bool] = None,
        path: Optional[str] = None,
        resource: Optional[Resource] = None,
        resource_cls: Optional[Type[Resource]] = None,
    ) -> None:
        """
		Add the given resource to the graph.
		"""
        if resource_cls is None and isinstance(snapshot, SchemaSnapshot):
            raise exc.GraphError(
                "`resource_cls` must be provided if `snapshot` is a snapshot instead of a resource."
            )

        # Simplify the syntax--allow adding resources, even though
        # we actually only add snapshots to the graph
        if isinstance(snapshot, Resource):
            resource = snapshot
            resource_cls = type(resource)
            snapshot = snapshot.snapshot

        if name is not None:
            resource._name = name  # pylint: disable=protected-access

        if resource_cls is None and resource is not None:
            resource_cls = type(resource)

        if path is None:
            path = self.session.path(resource)
        self._add_at_path(path, snapshot, exists, resource, resource_cls)

    def resolve_all(
        self, field_filter: Optional[Callable[[Field], bool]] = None, partial: bool = False,
    ) -> "ResourceGraph":
        """
		Resolve all entities or replace their snapshots with `None` if they can't be resolved
		"""
        if partial and field_filter is not None:
            raise ValueError("If `field_filter` is provided `partial` must be False.")

        instance = self.copy()

        def _method(snapshot):
            if partial:
                return snapshot.resolve_partial(instance)
            return snapshot.resolve(instance, field_filter)

        for path in instance.graph:
            out_data = instance.query(path, False)
            if out_data["snapshot"] is None:
                continue

            try:
                out_data["snapshot"] = _method(out_data["snapshot"])
            except exc.ResolutionError:
                if out_data.get("exists", False):
                    raise
                out_data["snapshot"] = None

        return instance
