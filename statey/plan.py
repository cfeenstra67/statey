import abc
import copy
import dataclasses as dc
from datetime import datetime
from itertools import chain
from typing import Optional

import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph

from statey.resource import ResourceSession, BoundState
from statey.syms import session, symbols, exc, utils
from statey.task import TaskSession, SessionSwitch


@dc.dataclass(frozen=True)
class Plan(abc.ABC):
	"""

	"""
	nested_task_graph: nx.DiGraph
	config_session: ResourceSession
	state_session: Optional[ResourceSession]

	def flatten_task_graph(
		self,
		output_session: session.Session,
		# this is required if self.state_session is not None
		state_output_session: Optional[session.Session] = None
	) -> 'TaskGraph':
		"""
		Given a directed resolution graph, we will build a task graph from its task
		sessions
		"""
		from statey.executor import TaskGraph

		if state_output_session is None and self.state_session is not None:
			raise ValueError('state_output_session must be provided when state_session is not None.')

		out_graph = nx.DiGraph()

		# First we want to fill in 2 things:
		# previous states
		# symbols in the output session
		if self.state_session is not None:
			for node in self.state_session.ns.keys():
				typ = self.state_session.ns.resolve(node)
				state_output_session.ns.new(node, typ)
				state_output_session.set_data(node, self.state_session.resolve(self.state_session.ns.ref(node)))

		config_nodes = {node for node in self.nested_task_graph if not self.nested_task_graph.nodes[node]['state_only']}
		for node in self.config_session.ns.keys():
			typ = self.config_session.ns.resolve(node)
			output_session.ns.new(node, typ)
			output_session.set_data(node, self.config_session.session.get_encoded_data(node))

		for node in topological_sort(self.nested_task_graph):
			data = self.nested_task_graph.nodes[node]

			use_output_session = state_output_session if data['state_only'] else output_session

			input_switch_task = SessionSwitch(
				input_session=use_output_session,
				input_symbol=data['input_symbol'],
				output_session=data['task_session'],
				output_key=data['input_key']
			)
			input_key = f'{node}:input'
			print("TASK", input_key, input_switch_task)
			out_graph.add_node(input_key, task=input_switch_task, source=node)

			# Add upstream edges
			for up_node in self.nested_task_graph.pred[node]:
				out_graph.add_edge(f'{up_node}:output', input_key)

			output_switch_task = SessionSwitch(
				input_session=data['task_session'],
				input_symbol=data['output_ref'],
				output_session=use_output_session,
				output_key=node,
				allow_unknowns=False,
				overwrite_output_type=data['config_state'].state.type
			)
			output_key = f'{node}:output'
			print("TASK2", output_key, output_switch_task)
			out_graph.add_node(output_key, task=output_switch_task, source=node)
 
			task_graph = data['task_session'].task_graph()
			if not task_graph.nodes:
				out_graph.add_edge(input_key, output_key)

			for sub_node in topological_sort(task_graph):
				node_key = f'{node}:task:{sub_node}'
				task = task_graph.nodes[sub_node]['task']
				out_graph.add_node(node_key, task=task, source=node)

				# If not predecessors, make it dependent on the input
				# switch
				if not task_graph.pred[sub_node]:
					out_graph.add_edge(input_key, node_key)
				# Otherwise, add upstream edges
				else:
					for pred in task_graph.pred[sub_node]:
						out_graph.add_edge(f'{node}:task:{pred}', node_key)

				# The opposite of above--if no dependencies, make the
				# output a dependency
				if not task_graph.succ[sub_node]:
					out_graph.add_edge(node_key, output_key)

		return TaskGraph(out_graph)


class Migrator(abc.ABC):
	"""
	Migrator is the interface for creating plans for migrating one set of resource states
	to another
	"""
	@abc.abstractmethod
	def create_task_session(self) -> TaskSession:
		"""
		Create a new instance of a TaskSession, used by resource classes to coordinate one
		or more actions in a grap symbollically
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def plan(self, config_session: ResourceSession, state_session: Optional[ResourceSession] = None) -> Plan:
		"""
		Create a task graph to create the resource states in config_session, migrating
		from state_session if it is passed
		"""
		raise NotImplementedError


class DefaultMigrator(Migrator):
	"""
	simple default Migrator implementation
	"""
	def create_task_session(self) -> TaskSession:
		return TaskSession(self.create_session())

	def create_session(self) -> session.Session:
		from statey.syms.py_session import create_session
		return create_session()

	def nested_task_graph(self, config_session: ResourceSession, state_session: Optional[ResourceSession] = None) -> Plan:

		config_graph = config_session.resource_graph()
		state_graph = state_session.resource_graph() if state_session else nx.MultiDiGraph()
		config_nodes = set(config_graph.nodes)
		state_nodes = set(state_graph.nodes)
		state_only_graph = state_graph.subgraph(state_nodes - config_nodes)

		resolution_graph = nx.DiGraph()
		edges = set()

		output_session = self.create_session()

		# First, fill any non-resource keys into the output session. We put the symbolic representation
		# only here, since these are not stateful and thus don't need to be resolved in any particular
		# order. They need to be added to the output session so that references work properly.
		config_other_keys = set(config_session.ns.keys()) - config_nodes
		for other_key in config_other_keys:
			typ = config_session.ns.resolve(other_key)
			output_session.ns.new(other_key, typ, overwrite=True)
			# TODO(cam): need to expose this method in the session API if it is required here,
			# or maybe say only python session can be used here? This is fine for the short term,
			# should think of something better though.
			output_session.set_data(other_key, config_session.session.get_encoded_data(other_key))

		# Next, go through the resources that only exist in the old state and do the planning
		# there, storing the edges to be added at the end
		for node in reversed(list(topological_sort(state_only_graph))):
			previous_state = state_only_graph.nodes[node]['state']
			resource = state_session.ns.registry.get_resource(previous_state.resource_name)
			previous_resolved = state_session.resolve(state_session.ns.ref(node), decode=False)
			config_state = resource.null_state

			for dependent_node in state_graph.succ[node]:
				edges.add((node, dependent_node))

			task_session = self.create_task_session()
			input_ref = task_session.ns.new('input', config_state.state.type)
			task_session.set_data('input', {})

			previous_bound = BoundState(data=previous_resolved, resource_state=previous_state)
			current_bound = BoundState(data={}, resource_state=config_state)

			output_ref = resource.plan(previous_bound, current_bound, task_session, input_ref)
			# This needs to be fully resolved
			output_resolved = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

			resolution_graph.add_node(
				node,
				expected_data=output_resolved,
				previous_data=previous_resolved,
				task_session=task_session,
				input_key='input',
				output_ref=output_ref,
				config_state=config_state,
				previous_state=previous_state,
				resource=resource,
				input_symbol=symbols.Literal(
					value={},
					type=config_state.state.type,
					registry=state_session.ns.registry
				),
				state_only=True
			)

		# Next, go through the configured resources in topological sort order
		for node in topological_sort(config_graph):
			config_state = config_graph.nodes[node]['state']
			resource = config_session.ns.registry.get_resource(config_state.resource_name)
			previous_state = state_graph.nodes[node]['state'] if node in state_graph else resource.null_state
			try:
				previous_resolved = state_session.resolve(state_session.ns.ref(node), decode=False) if state_session else {}
			except exc.SymbolKeyError:
				previous_resolved = {}

			output_session.ns.new(node, config_state.state.type)
			# Again, this is not ideal
			output_session.set_data(node, config_session.session.get_encoded_data(node))

			partial_resolved = output_session.resolve(output_session.ns.ref(node), allow_unknowns=True, decode=False)

			# use the partially resolved symbolic representation to get the unresolved dependencies
			output_session.set_data(node, partial_resolved)
			dep_graph = output_session.dependency_graph()
			utils.subgraph_retaining_dependencies(dep_graph, config_nodes)

			for other in dep_graph.pred[node]:
				resolution_graph.add_edge(other, node)

			task_session = self.create_task_session()
			input_ref = task_session.ns.new('input', config_state.state.type)
			task_session.set_data('input', partial_resolved)

			previous_bound = BoundState(data=previous_resolved, resource_state=previous_state)
			current_bound = BoundState(data=partial_resolved, resource_state=config_state)

			output_ref = resource.plan(previous_bound, current_bound, task_session, input_ref)
			output_partial = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

			def add_refs(x):
				if isinstance(x, symbols.Unknown):
					return x.clone(refs=(output_session.ns.ref(node),))
				return x

			semantics = output_session.ns.registry.get_semantics(config_state.state.type)
			output_partial = semantics.map(add_refs, output_partial)

			# Set the result of planning for downstream dependencies
			output_session.set_data(node, output_partial)

			resolution_graph.add_node(
				node,
				expected_data=output_partial,
				previous_data=previous_resolved,
				task_session=task_session,
				input_key='input',
				output_ref=output_ref,
				config_state=config_state,
				previous_state=previous_state,
				resource=resource,
				input_symbol=config_session.ns.ref(node),
				state_only=False
			)

		for node, dependent_node in edges:
			if (dependent_node, node) not in resolution_graph.edges:
				resolution_graph.add_edge(node, dependent_node)

		if not is_directed_acyclic_graph(resolution_graph):
			raise ValueError(f'{repr(resolution_graph)} is not a DAG!')

		return resolution_graph

	def plan(self, config_session: ResourceSession, state_session: Optional[ResourceSession] = None) -> Plan:
		return Plan(
			nested_task_graph=self.nested_task_graph(config_session, state_session),
			config_session=config_session,
			state_session=state_session
		)
