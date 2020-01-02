from typing import Optional, Union, Type

import networkx as nx

from .resource import Resource
from statey import exc
from statey.schema import Field, SchemaSnapshot, Symbol, QueryRef
from statey.session import Session


class ResourceGraph:
	"""
	A ResourceGraph provides public methods to build a NetworkX graph
	object that is used for operations like plan(), apply(), etc.
	"""
	def __init__(self, session: Session) -> None:
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
			return node['snapshot']
		return node

	def _path(self, ref: QueryRef) -> str:
		return self.session.path(resource) if isinstance(resource, Resource) else resource

	def _add_at_path(
			self,
			path: str,
			snapshot: SchemaSnapshot,
			exists: Optional[bool],
			resource_cls: Type[Resource]
	) -> None:
		"""
		Add the given snapshot at the given path. This operation is atomic, so either
		it succeeds entirely or fails entirely
		"""
		if path in self.graph:
			raise exc.GraphIntegrityError(
				f'Attempting to insert snapshot into the graph for path "{path}",'
				f' but a snapshot already exists at this path.'
			)

		new_graph = self.graph.copy()
		kws = {'snapshot': snapshot, 'resource_cls': resource_cls}
		if exists is not None:
			kws['exists'] = exists
		new_graph.add_node(path, **kws)

		for name, value in snapshot.items():
			# Only need special behavior for symbols, otherwise
			# no additional analysis required
			if not isinstance(value, Symbol):
				continue

			for resource, field in value.refs():
				resource_path = self._path(resource)
				data = self.query(resource_path, False)
				resource_cls = data['resource_cls']

				if field not in resource_cls.Schema.__fields__:
					raise exc.InvalidReference(
						f'Field {repr(field)} does not belong to schema {repr(resource_cls.Schema)}'
						f' of resource class {repr(resource_cls)}.'
					)

				if resource_path not in new_graph:
					raise exc.InvalidReference(
						f'Failed to add snapshot {repr(snapshot)} at path {path}. The field {repr(name)}'
						f' references a path not yet added to this graph: {resource_path}.'
					)

				new_graph.add_edge(
					(resource_path, path),
					symbol=value,
					destination_field_name=name,
					field=field
				)

		# Only update self.graph if we get through the add operation successfully
		self.graph = new_graph

	def add(
			self,
			snapshot: Union[Resource, SchemaSnapshot],
			name: Optional[str] = None,
			exists: Optional[bool] = None,
			path: Optional[str] = None,
			resource_cls: Optional[Type[Resource]] = None
	) -> None:
		"""
		Add the given resource to the graph.
		"""
		if resource_cls is None and isinstance(snapshot, SchemaSnapshot):
			raise exc.GraphError('`resource_cls` must be provided if `snapshot` is a snapshot instead of a resource.')

		# Simplify the syntax--allow adding resources, even though
		# we actually only add snapshots to the graph
		if isinstance(snapshot, Resource):
			resource_cls = type(snapshot)
			snapshot = snapshot.snapshot

		if name is not None:
			snapshot.resource._name = name

		if path is None:
			path = self.session.path(snapshot.resource)
		self._add_at_path(path, snapshot, exists, resource_cls)
