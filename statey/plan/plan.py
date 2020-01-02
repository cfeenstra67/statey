from typing import Sequence, Tuple, Optional

import networkx as nx

from .change import Change, NoChange, Create, Delete, DeleteAndRecreate, Update
from .executor import PlanExecutor, PlanJob, DefaultPlanExecutor
from statey import exc
from statey.schema import Symbol, SchemaSnapshot
from statey.session import Session
from statey.resource import ResourceGraph, Resource


class Plan:
	"""
	A statey plan contains all information required to apply any quantity
	of changes to a resource graph
	"""
	def __init__(
			self,
			config_graph: ResourceGraph,
			state_graph: Optional[ResourceGraph] = None
	) -> None:
		"""
		`config_graph` - The ResourceGraph with the desired configuration for this 
		"""
		self.config_graph = config_graph
		self.resolve_graph = self.config_graph.copy()
		self.state_graph = state_graph
		self.graph = mx.DiGraph(plan=self)

		self.max_depth = -1

	def _get_resource(self, path: str, snapshot: SchemaSnapshot) -> Resource:
		if snapshot.resource is None:
			raise exc.MissingResourceError(f'Path: {path}. Snapshot: {snapshot}.')
		return snapshot.resource

	def process_node(self, path: str, ancestors: Sequence[str]) -> None:
		"""
		Process the given node. Note that all ancestors of the given node must
		already exist in the graph
		"""
		snapshot = (
			self.resolve_graph.graph.nodes[path]['snapshot'] 
		) = (
			self.config_graph.graph.nodes[path]['snapshot'].resolve_partial(self.resolve_graph)
		)
		config = self.config_graph.nodes[path]['snapshot']
		resource = self._get_resource(path, config)

		if (
			self.state_graph is None
			or path not in self.state_graph.graph
			or not self.state_graph.graph.nodes[path].get('exists', False)
		):
			change = Create(resource, snapshot, None)
		else:
			old_snapshot = self.state_graph.query(snapshot.resource)

			changes = {}
			recreate = False

			for key, new_value in snapshot.items():
				old_value = old_snapshot[key]
				if isinstance(new_value, Symbol) or old_value != new_value:
					field = snapshot.resource.Schema.__fields__[key]
					changes[field] = old_value, new_value
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

	def process_delete_node(self, path: str, successors: Sequence[str]) -> None:
		"""
		Process a given path for deletion. Note that all successors of the given
		node must already exist in the graph
		"""
		snapshot = self.state_graph.graph[path]['snapshot']
		resource = self._get_resource(path, snapshot)

		self.graph.add_node(path, change=Delete(resource, None, old_snapshot))
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
			key: self.state_graph.graph.successors(key)
			for key in self.state_graph.graph
		}
		max_depth = -1

		while len(not_processed) > 0:
			max_depth += 1

			for path in not_processed:
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
		ancestor_map = {
			key: self.graph.predecessors(key)
			for key in self.graph
		}
		for path, ancestors in ancestor_map.items():
			for ancestor in ancestors:
				ancestor_ancestors = ancestor_map[ancestor]
				if path in ancestor_ancestors:
					raise exc.GraphIntegrityError(
						f'Circular reference detected in plan between resources'
						f' "{path}" and "{ancestor}".'
					)

	def build(self) -> None:
		"""
		Build a directed graph of changes based on resource dependencies
		"""
		not_processed = set(self.config_graph.graph)
		ancestor_map = {
			key: self.config_graph.graph.predecessors(key)
			for key in self.config_graph.graph
		}
		max_depth = -1

		while len(not_processed) > 0:
			max_depth += 1

			for path in not_processed:
				ancestors = ancestor_map[path]

				if set(ancestors) & not_processed:
					continue

				self.process_node(path, ancestors)
				not_processed.remove(path)

		self.max_depth = max(self.max_depth, max_depth)
		self.add_deletions()
		self.validate()

	def apply(self, executor: PlanExecutor = DefaultPlanExecutor()) -> Tuple[PlanJob, ResourceGraph]:
		"""
		Execute the current plan as a job using the given executor
		"""
		job = PlanJob()
		ancestor_map = {
			key: self.graph.predecessors(key)
			for key in self.graph
		}
		successor_map = {
			key: self.graph.successors(key)
			for key in self.graph
		}
		state_graph = self.config_graph.copy()

		def complete(path, change, snapshot):
			state_graph.nodes[path]['snapshot'] = snapshot
			state_graph.nodes[path]['exists'] = True

			complete = {key: val for key, val in job.complete}
			errors = {key: val for key, val, _ in job.errors}

			for successor_path in successor_map[path]:
				ancestors = ancestor_map[successor_path]

				# This shouldn't happen, because the job should already be aborted.
				# Just here to be safe
				if set(ancestors) & set(errors):
					job.abort()
					return

				# There are still unfinished ancestors, so don't queue this
				# item yet
				if set(ancestors) - set(complete):
					continue

				# We should get here exactly once for each path (across all calls of complete())
				job.add_change(successor_path, self.graph.nodes[successor_path]['change'])

		job.complete_callback = complete

		def error(path, change, error):
			state_graph.nodes[path]['error'] = error
			if self.state_graph is not None and path in self.state_graph.graph:
				state_graph.nodes[path]['snapshot'] = self.state_graph.nodes[path]['snapshot']
				state_graph.nodes[path]['exists'] = self.state_graph.nodes[path].get('exists', False)
			else:
				state_graph.nodes[path]['snapshot'] = None
				state_graph.nodes[path]['exists'] = False

			job.abort()

		job.error_callback = error

		def change(path, change):
			change.snapshot = change.snapshot.resolve(state_graph)
			return change

		job.change_hook = change

		return executor.execute(job), state_graph
