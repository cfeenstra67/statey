import abc
import copy
import dataclasses as dc
from itertools import chain
from typing import Optional

import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph

from statey.resource import ResourceSession, BoundState
from statey.syms import session, symbols
from statey.task import TaskSession, SessionSwitch


@dc.dataclass(frozen=True)
class Plan(abc.ABC):
	"""

	"""
	nested_task_graph: nx.DiGraph
	flat_task_graph: nx.DiGraph
	state_session: ResourceSession
	config_session: Optional[ResourceSession]


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
		from statey.syms.py_session import create_session
		return TaskSession(create_session())

	def nested_task_graph(self, config_session: ResourceSession, state_session: Optional[ResourceSession] = None) -> Plan:
		output_session = config_session.clone()

		config_graph = output_session.resource_graph()
		state_graph = state_session.resource_graph() if state_session else nx.MultiDiGraph()
		config_nodes = set(config_graph.nodes)
		state_nodes = set(state_graph.nodes)
		state_only_graph = state_graph.subgraph(state_nodes - config_nodes)

		resolution_graph = nx.DiGraph()

		for node in topological_sort(config_graph):
			config_state = config_graph.nodes[node]['state']
			resource = output_session.ns.registry.get_resource(config_state.resource_name)
			previous_state = state_graph.nodes[node]['state'] if node in state_graph else resource.null_state
			previous_resolved = state_session.resolve(state_session.ns.ref(node), decode=False) if state_session else {}
			partial_resolved = output_session.resolve(config_session.ns.ref(node), allow_unknowns=True, decode=False)

			# use the partially resolved symbolic representation to get the unresolved dependencies
			output_session.set_data(node, partial_resolved)
			dep_graph = output_session.resource_graph()

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

			# Set the result of planning for upstream dependencies
			output_session.set_data(node, output_partial)

			resolution_graph.add_node(
				node,
				task_session=task_session,
				input_key='input',
				output_ref=output_ref,
				config_state=config_state,
				previous_state=previous_state,
				resource=resource,
				session=output_session,
				input_symbol=config_session.ns.ref(node)
			)

		for node in reversed(list(topological_sort(state_only_graph))):
			previous_state = state_only_graph.nodes[node]['state']
			resource = state_session.ns.registry.get_resource(previous_state.resource_name)
			previous_resolved = state_session.resolve(state_session.ns.ref(node), decode=False)
			config_state = resource.null_state
			dep_graph = state_session.resource_graph()

			for dependent_node in dep_graph.succ[node]:
				if (dependent_node, node) not in resolution_graph.edges:
					resolution_graph.add_edge(node, dependent_node)

			task_session = self.create_task_session()
			input_ref = task_session.ns.new('input', config_state.state.type)
			task_session.set_data('input', {})

			previous_bound = BoundState(data=previous_resolved, resource_state=previous_state)
			current_bound = BoundState(data={}, resource_state=config_state)

			output_ref = resource.plan(previous_bound, current_bound, task_session, input_ref)
			# This needs to be fully resolved
			output_partial = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

			resolution_graph.add_node(
				node,
				task_session=task_session,
				input_key='input',
				output_ref=output_ref,
				config_state=config_state,
				previous_state=previous_state,
				resource=resource,
				session=output_session,
				input_symbol=symbols.Literal(
					value={},
					type=config_state.state.type,
					registry=state_session.ns.registry
				)
			)

		if not is_directed_acyclic_graph(resolution_graph):
			raise ValueError(f'{resolution_graph} is not a DAG!')

		print("EDGES", resolution_graph.edges)

		return resolution_graph

	def flatten_task_graph(self, nested_graph: nx.DiGraph) -> nx.DiGraph:
		"""
		Given a directed resolution graph, we will build a task graph from its task
		sessions
		"""
		out_graph = nx.DiGraph()

		for node in topological_sort(nested_graph):
			data = nested_graph.nodes[node]

			input_switch_task = SessionSwitch(
				input_session=data['session'],
				input_symbol=data['input_symbol'],
				output_session=data['task_session'],
				output_key=data['input_key']
			)
			input_key = f'{node}:input'
			out_graph.add_node(input_key, task=input_switch_task, source=node)

			# Add upstream edges
			for up_node in nested_graph.pred[node]:
				out_graph.add_edge(f'{up_node}:output', input_key)

			output_switch_task = SessionSwitch(
				input_session=data['task_session'],
				input_symbol=data['output_ref'],
				output_session=data['session'],
				output_key=node
			)
			output_key = f'{node}:output'
			out_graph.add_node(output_key, task=output_switch_task, source=node)
 
			task_graph = data['task_session'].task_graph()

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

		return out_graph

	def plan(self, config_session: ResourceSession, state_session: Optional[ResourceSession] = None) -> Plan:
		nested_graph = self.nested_task_graph(config_session, state_session)
		flat_graph = self.flatten_task_graph(nested_graph)
		return Plan(
			nested_task_graph=nested_graph,
			flat_task_graph=flat_graph,
			config_session=config_session,
			state_session=state_session
		)
