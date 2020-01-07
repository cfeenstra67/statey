"""
A ResouceGraph is one of the core data structures in statey. It provides an interface
for building and operating on resource graphs
"""
import itertools
from typing import Optional, Union, Type, Callable, Dict, Any

import networkx as nx

from statey import exc
from statey.schema import (
    Field,
    SchemaSnapshot,
    Symbol,
    QueryRef,
    CacheManager,
    SchemaHelper,
    Reference,
    Literal,
)
from .path import Registry
from .resource import Resource


class ResourceGraph:
    """
	A ResourceGraph provides public methods to build a NetworkX graph
	object that is used for operations like plan(), apply(), etc.
	"""

    def __init__(self, registry: Registry) -> None:
        """
		`registry` - a statey registry
		"""
        self.registry = registry
        self.graph = nx.MultiDiGraph(resource_graph=self)

    def query(self, resource: QueryRef, snapshot_only: bool = True) -> SchemaSnapshot:
        """
		Query a specific field value from the graph
		"""
        path = self._path(resource)
        if path not in self.graph:
            raise exc.InvalidReference(path)

        node = self.graph.nodes[self._path(resource)]
        if snapshot_only:
            return node["snapshot"]
        return node

    def copy(self) -> "ResourceGraph":
        """
		Return a copy of this resource graph
		"""
        graph = type(self)(self.registry)
        graph.graph = self.graph.copy()
        return graph

    def _path(self, resource: QueryRef) -> str:
        return (
            self.registry.get_path(resource)
            if isinstance(resource, Resource)
            else resource
        )

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

        if snapshot.__schema__ is not resource_cls.Schema:
            raise exc.GraphIntegrityError(
                f'Attempting to insert snapshot into the graph for path "{path}",'
                f" but the snapshot's schema does not match resource_cls.Schema "
                f"(got {repr(snapshot.__schema__)}, expected {resource_cls.Schema})."
            )

        if resource is not None and not isinstance(resource, resource_cls):
            raise exc.GraphIntegrityError(
                f'Attempt to insert snapshot into the graph for path "{path}",'
                f" but the snapshot's resource is not of type resource_cls "
                f"(got {repr(type(resource))}, expected subclass of "
                f"{repr(resource_cls)}."
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

            for other_resource, field_path in value.refs():
                resource_path = self._path(other_resource)
                # Ignore self references
                if resource_path == path:
                    continue
                data = self.query(resource_path, False)
                other_resource_cls = data["resource_cls"]

                if (
                    field_path not in other_resource_cls.Schema
                    or resource_path not in new_graph
                ):
                    raise exc.InvalidReference(
                        resource_path, field_path[-1], field_path[:-1]
                    )

                new_graph.add_edge(
                    resource_path,
                    path,
                    symbol=value,
                    destination_field_name=name,
                    field=field_path[-1],
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
                "`resource_cls` must be provided if `snapshot` is a snapshot instead"
                " of a resource."
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
            path = self.registry.get_path(resource)
        self._add_at_path(path, snapshot, exists, resource, resource_cls)

    def resolve_all(
        self,
        field_filter: Optional[Callable[[Field], bool]] = None,
        partial: bool = False,
        cache: Optional[CacheManager] = None,
    ) -> "ResourceGraph":
        """
		Resolve all entities or replace their snapshots with `None` if they can't be resolved
		"""
        if partial and field_filter is not None:
            raise ValueError("If `field_filter` is provided `partial` must be False.")

        instance = self.copy()
        cache = CacheManager() if cache is None else cache

        def _method(snapshot):
            if partial:
                return snapshot.resolve_partial(instance, cache)
            return snapshot.resolve(instance, field_filter, cache)

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

    # pylint: disable=too-many-locals
    def to_dict(self, meta: Optional[Any] = None) -> Dict[str, Any]:
        """
        Dump the graph to a serializable dictionary.
        This is a custom format intended to be human-readable
        """
        nodes = {}

        dependencies = {}
        reverse_dependencies = {}
        for src, dest, data in self.graph.edges(data=True):
            dependencies.setdefault(dest, {}).setdefault(src, []).append(data)
            reverse_dependencies.setdefault(src, {}).setdefault(dest, []).append(data)

        for node in self.graph:
            depends_on = dependencies.get(node, {})
            data = self.query(node, False)
            # Do not write null states at all, but check to make sure that the
            # graph still makes sense if we do that. Null states can also be the
            # result of an upstream bug, so better to catch that before writing than
            # after.
            if data["snapshot"] is None:
                depending = [
                    path
                    for path in sorted(reverse_dependencies.get(node, []))
                    if self.query(path) is not None
                ]
                if len(depending) > 0:
                    raise exc.GraphIntegrityError(
                        f"Resource at path {node} has a null state, but it is "
                        f"dependended on by resource(s) will non-null states: "
                        f"{depending}. This is unexpected!"
                    )
                continue

            snapshot, exists = data["snapshot"], data.get("exists", False)
            snapshot_data = snapshot and SchemaHelper(snapshot.__schema__).dump(
                snapshot
            )

            deps = {}
            for src, edges in depends_on.items():
                if src == node:
                    continue

                for edge in edges:
                    field_deps = deps.setdefault(
                        edge["destination_field_name"], {}
                    ).setdefault(src, set())
                    field_deps.add(edge["field"])

            deps = {
                k: {k2: sorted(v2) for k2, v2 in v.items()} for k, v in deps.items()
            }

            nodes[node] = {
                "name": data["resource"].name,
                "type_name": data["resource"].type_name,
                "snapshot": snapshot_data,
                "exists": exists,
                "dependencies": deps,
                "lazy": sorted(snapshot.__meta__.get("lazy", [])),
            }

        if meta is None:
            meta = {}

        return {"meta": meta, "resources": nodes}

    # pylint: disable=too-many-locals
    def _load_resource_from_dict(self, path: str, data: Dict[str, Any]) -> None:
        resource_cls = Resource.find(data["type_name"])
        if resource_cls is None:
            raise exc.UndefinedResourceType(data["type_name"])

        schema_helper = SchemaHelper(resource_cls.Schema)

        snapshot_data = schema_helper.load(data["snapshot"])
        graph_data = {}

        for field, value in snapshot_data.items():
            if field not in data["dependencies"]:
                graph_data[field] = value
                continue

            refs = []
            for src_path, src_fields in data["dependencies"][field].items():
                for src_field in src_fields:
                    other = self.query(src_path, False)
                    field_obj = other["resource_cls"].Schema.__fields__[src_field]

                    refs.append(
                        Reference(
                            resource=src_path, field_name=src_field, field=field_obj
                        )
                    )

            field_obj = resource_cls.Schema.__fields__[field]
            graph_data[field] = Literal(value=value, type=field_obj, refs=tuple(refs))

        snapshot = schema_helper.load(graph_data, validate=False)
        snapshot.__meta__["lazy"] = data.get("lazy", [])
        resource = resource_cls.from_snapshot(snapshot)
        snapshot = snapshot.fill_missing_values(resource)

        self.add(
            snapshot=snapshot,
            name=data["name"],
            exists=data["exists"],
            path=path,
            resource=resource,
            resource_cls=resource_cls,
        )

    def from_dict(self, data: Dict[str, Any]) -> None:
        """
        Construct a ResourceGraph given a dictionary constructed vai dump_to_dict()
        """
        not_processed = set(data["resources"])

        def deps_paths(deps):
            values = map(set, deps.values())
            values = itertools.chain.from_iterable(values)
            return set(values)

        while len(not_processed) > 0:

            for path in sorted(not_processed):
                resource = data["resources"][path]
                if deps_paths(resource["dependencies"]) & not_processed:
                    continue
                node = data["resources"][path]
                # If no state already exists we don't need to add it to the graph
                if node["snapshot"] is None:
                    raise exc.NullStateError(path)
                self._load_resource_from_dict(path, node)
                not_processed.remove(path)
