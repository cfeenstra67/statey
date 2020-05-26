import abc
import copy
import dataclasses as dc
from datetime import datetime
from functools import reduce
from itertools import chain
from typing import Optional, Sequence, Dict, Tuple, Any

import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph

from statey.resource import ResourceSession, BoundState, ResourceState, Resource, ResourceGraph
from statey.syms import session, symbols, exc, utils, types
from statey.task import TaskSession, SessionSwitch, Checkpointer, GraphDeleteKey, GraphSetKey


class PlanAction(abc.ABC):
	"""
	A plan action is some subset of a full task graph for a given name.
	"""
	@abc.abstractmethod
	def render(
		self,
		graph: nx.DiGraph,
		prefix: str,
		resource_graph: ResourceGraph,
		output_session: session.Session,
		# Dependencies of the configuration
		dependencies: Sequence[str]
	) -> None:
		"""
		Add the tasks in this graph to the given graph. Note that the output
		node must be {prefix}:output
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def input_task(self, prefix: str) -> str:
		"""
		Return the name of the task, given a prefix, that will be the "input" task when
		rendering this action
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def output_task(self, prefix: str) -> str:
		"""
		Return the name of the task, given a prefix, that will be the "output" task when
		rendering this action
		"""
		raise NotImplementedError


@dc.dataclass(frozen=True)
class ExecuteTaskSession(PlanAction):
	"""

	"""
	task_session: TaskSession
	task_input_key: str
	output_ref: symbols.Symbol
	output_key: str
	config_state: ResourceState
	previous_state: ResourceState
	resource: Resource
	input_symbol: symbols.Symbol

	def input_task(self, prefix: str) -> str:
		return f'{prefix}:input'

	def output_task(self, prefix: str) -> str:
		return f'{prefix}:output'

	def render(
		self,
		graph: nx.DiGraph,
		prefix: str,
		resource_graph: ResourceGraph,
		output_session: session.Session,
		# Dependencies of the configuration
		dependencies: Sequence[str]
	) -> None:
		task_graph = self.task_session.task_graph()

		input_switch_task = SessionSwitch(
			input_session=output_session,
			input_symbol=self.input_symbol,
			output_session=self.task_session,
			output_key=self.task_input_key
		)

		input_key = self.input_task(prefix)
		graph.add_node(input_key, task=input_switch_task, source=prefix)

		# If output state is null, our graph operation is a deletion. Otherwise, it's a "set"
		if self.config_state == self.resource.null_state:
			output_key = self.output_task(prefix)
			output_graph_task = GraphDeleteKey(
				key=self.output_key,
				resource_graph=resource_graph
			)
			graph.add_node(output_key, task=output_graph_task, source=prefix)
		else:
			output_key = f'{prefix}:state'
			output_graph_task = GraphSetKey(
				input_session=self.task_session,
				input_symbol=self.output_ref,
				dependencies=dependencies,
				key=self.output_key,
				resource_graph=resource_graph,
				state=self.config_state
			)
			graph.add_node(output_key, task=output_graph_task, source=prefix)

			output_switch_task = SessionSwitch(
				input_session=self.task_session,
				input_symbol=self.output_ref,
				output_session=output_session,
				output_key=self.output_key
			)
			output_switch_key = self.output_task(prefix)
			graph.add_node(output_switch_key, task=output_switch_task, source=prefix)

			# We should always update the state before the session
			graph.add_edge(output_key, output_switch_key)

		task_graph = self.task_session.task_graph()
		if not task_graph.nodes:
			graph.add_edge(input_key, output_key)

		task_key = lambda x: f'{prefix}:task:{x}'

		for sub_node in topological_sort(task_graph):
			node_key = task_key(sub_node)
			task = task_graph.nodes[sub_node]['task']

			graph.add_node(node_key, task=task, source=prefix)

			# If not predecessors, make it dependent on the input
			# switch
			if not task_graph.pred[sub_node]:
				graph.add_edge(input_key, node_key)
			# Otherwise, add upstream edges
			else:
				for pred in task_graph.pred[sub_node]:
					graph.add_edge(task_key(pred), node_key)

			# The opposite of above--if no dependencies, make the
			# output a dependency
			if not task_graph.succ[sub_node]:
				graph.add_edge(node_key, output_key)


@dc.dataclass(frozen=True)
class SetValue(PlanAction):
	"""
	
	"""
	output_key: str
	output_symbol: symbols.Symbol

	def input_task(self, prefix: str) -> str:
		return f'{prefix}:state'

	def output_task(self, prefix: str) -> str:
		return f'{prefix}:state'

	def render(
		self,
		graph: nx.DiGraph,
		prefix: str,
		resource_graph: ResourceGraph,
		output_session: session.Session,
		# Dependencies of the configuration
		dependencies: Sequence[str]
	) -> None:

		graph_task_name = self.input_task(prefix)
		graph_task = GraphSetKey(
			input_session=output_session,
			key=self.output_key,
			input_symbol=self.output_symbol,
			resource_graph=resource_graph,
			dependencies=dependencies
		)
		graph.add_node(graph_task_name, task=graph_task, source=prefix)


@dc.dataclass(frozen=True)
class DeleteValue(PlanAction):
	"""

	"""
	delete_key: str

	def input_task(self, prefix: str) -> str:
		return f'{prefix}:state'

	def output_task(self, prefix: str) -> str:
		return f'{prefix}:state'

	def render(
		self,
		graph: nx.DiGraph,
		prefix: str,
		resource_graph: ResourceGraph,
		output_session: session.Session,
		# Dependencies of the configuration
		dependencies: Sequence[str]
	) -> None:

		graph_task_name = self.input_task(prefix)
		graph_task = GraphDeleteKey(
			key=self.delete_key,
			resource_graph=resource_graph
		)
		graph.add_node(graph_task_name, task=graph_task, source=prefix)


@dc.dataclass(frozen=True)
class PlanNode:
	"""
	A plan node contains information about the state of a given name and before
	and after this migration, including any unknowns. It also contains information
	about the up and downstream dependencies of the configuration and previous state
	respectively
	"""
	key: str
	# This will always be fully resolved
	current_data: Any
	current_type: types.Type
	current_state: Optional[ResourceState]
	current_depends_on: Sequence[str]
	current_action: Optional[PlanAction]
	# This may contain unknowns
	config_data: Any
	config_type: types.Type
	config_state: Optional[ResourceState]
	config_depends_on: Sequence[str]
	config_action: Optional[PlanAction]

	def input_task(self) -> str:
		"""

		"""
		action = self.current_action or self.config_action
		return action.input_task(self.current_prefix())

	def current_prefix(self) -> str:
		"""

		"""
		if self.current_action is not None and self.config_action is not None:
			return f'{self.key}:current'
		return self.key

	def current_output_task(self) -> str:
		"""
		The output task of migrating the current state. This may or may not be the
		same as output_task(); the case where it is the same is when either this key
		only exists in one of the previous of configured namespaces or when it is a single
		resource whose state is being migrated.
		"""
		action = self.current_action or self.config_action
		return action.output_task(self.current_prefix())

	def config_prefix(self) -> str:
		"""

		"""
		if self.current_action is not None and self.config_action is not None:
			return f'{self.key}:config'
		return self.key

	def config_input_task(self) -> str:
		"""

		"""
		action = self.config_action or self.current_action
		return action.input_task(self.config_prefix())

	def output_task(self) -> str:
		"""

		"""
		action = self.config_action or self.current_action
		return action.output_task(self.config_prefix())

	def get_edges(self, other_nodes: Dict[str, 'PlanNode']) -> Sequence[Tuple[str, str]]:
		"""
		Get the edges for this node, given the other nodes.
		"""
		edges = set()

		input_task = self.input_task()
		current_output_task = self.current_output_task()

		for upstream in self.config_depends_on:
			node = other_nodes[upstream]
			ref = node.output_task()

			edges.add((ref, input_task))

		for downstream in self.current_depends_on:
			node = other_nodes[downstream]
			current_ref = node.current_output_task()
			config_ref = node.output_task()

			# So we are checking if our input/config currently depends on the _current_
			# output of the other task. If not, we'll make the other input dependent
			# on the _current output task_
			if not {(current_ref, input_task), (config_ref, input_task)} & edges:
				input_ref = node.input_task()
				edges.add((current_output_task, input_ref))

		return edges

	def get_task_graph(
		self,
		resource_graph: ResourceGraph,
		output_session: session.Session
	) -> nx.DiGraph:
		"""
		Render a task graph for this specific node of the plan. The names specified
		by current_output_task() and output_task() should be tasks in this graph.
		"""
		tasks = nx.DiGraph()

		if self.current_action is not None and self.config_action is not None:
			self.current_action.render(
				graph=tasks,
				prefix=self.current_prefix(),
				resource_graph=resource_graph,
				output_session=output_session,
				# The current task will always have a null result
				dependencies=()
			)
			self.config_action.render(
				graph=tasks,
				prefix=self.config_prefix(),
				resource_graph=resource_graph,
				output_session=output_session,
				dependencies=self.config_depends_on
			)
			# Make these two graphs depend on one another
			tasks.add_edge(self.current_output_task(), self.config_input_task())
		elif self.current_action:
			self.current_action.render(
				graph=tasks,
				prefix=self.current_prefix(),
				resource_graph=resource_graph,
				output_session=output_session,
				dependencies=()
			)
		else:
			self.config_action.render(
				graph=tasks,
				prefix=self.config_prefix(),
				resource_graph=resource_graph,
				output_session=output_session,
				dependencies=self.config_depends_on
			)

		return tasks


@dc.dataclass(frozen=True)
class Plan:
	nodes: Sequence[PlanNode]
	config_session: ResourceSession
	state_graph: ResourceGraph

	def task_graph(self) -> 'TaskGraph':
		"""
		Render a full task graph from this plan
		"""
		from statey.executor import TaskGraph

		state_graph = self.state_graph.clone()
		output_session = self.config_session.clone()

		node_dict = {node.key: node for node in self.nodes}

		graphs = []
		edges = set()

		for node in self.nodes:
			graphs.append(node.get_task_graph(
				resource_graph=state_graph,
				output_session=output_session
			))
			edges |= node.get_edges(node_dict)

		# This will raise an error if there are overlapping keys, which there
		# shouldn't be.
		full_graph = reduce(nx.union, graphs) if graphs else nx.DiGraph()
		full_graph.add_edges_from(edges)

		if not is_directed_acyclic_graph(full_graph):
			raise ValueError(f'{repr(full_graph)} is not a DAG!')

		return TaskGraph(full_graph, output_session, state_graph)


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

	def plan(self, config_session: ResourceSession, state_graph: Optional[ResourceGraph] = None) -> Plan:
		config_dep_graph = config_session.dependency_graph()
		state_dep_graph = state_graph.graph

		# For planning we need a copy of the config session that we will partially resolve
		# (maybe w/ unknowns) as we go
		output_session = config_session.clone()

		config_nodes = set(config_dep_graph.nodes)
		state_nodes = set(state_dep_graph.nodes)

		state_only_nodes = state_nodes - config_nodes

		plan_nodes = []

		for node in chain(topological_sort(config_dep_graph), state_only_nodes):
			previous_exists = node in state_nodes
			previous_state = None
			if node in state_dep_graph.nodes:
				previous_state = state_dep_graph.nodes[node]['state']

			config_exists = node in config_nodes
			config_state = None
			try:
				config_state = output_session.resource_state(node)
			except exc.SymbolKeyError:
				pass

			current_depends_on = list(state_dep_graph.pred[node]) if previous_exists else []
			config_depends_on = list(config_dep_graph.pred[node]) if config_exists else []

			# We can handle this node with one action--otherwise we need two, though
			# one or both may just be state update operations.
			if (
				config_state is not None
				and previous_state is not None
				and config_state.resource_name == previous_state.resource_name
			):
				resource = config_session.ns.registry.get_resource(config_state.resource_name)
				previous_resolved = state_dep_graph.nodes[node]['value']
				config_partial_resolved = config_session.resolve(config_session.ns.ref(node), decode=False, allow_unknowns=True)

				previous_bound = BoundState(data=previous_resolved, resource_state=previous_state)
				config_bound = BoundState(data=config_partial_resolved, resource_state=config_state)

				task_session = self.create_task_session()
				input_ref = task_session.ns.new('input', config_state.state.type)
				task_session.set_data('input', config_partial_resolved)

				output_ref = resource.plan(previous_bound, config_bound, task_session, input_ref)

				output_partial_resolved = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

				action = ExecuteTaskSession(
					task_session=task_session,
					task_input_key='input',
					output_key=node,
					output_ref=output_ref,
					config_state=config_state,
					previous_state=previous_state,
					resource=resource,
					input_symbol=config_session.ns.ref(node)
				)

				plan_node = PlanNode(
					key=node,
					current_data=previous_resolved,
					current_type=previous_state.state.type,
					current_state=previous_state,
					current_depends_on=current_depends_on,
					current_action=None,
					config_data=output_partial_resolved,
					config_type=config_state.state.type,
					config_state=config_state,
					config_depends_on=config_depends_on,
					config_action=action
				)
				plan_nodes.append(plan_node)
				continue

			args = {
				'key': node,
				'current_data': None,
				'current_type': None,
				'current_state': None,
				'current_depends_on': current_depends_on,
				'current_action': None,
				'config_data': None,
				'config_type': None,
				'config_state': None,
				'config_depends_on': config_depends_on,
				'config_action': None
			}

			if previous_state:
				resource = config_session.ns.registry.get_resource(previous_state.resource_name)
				previous_resolved = state_dep_graph.nodes[node]['value']

				previous_bound = BoundState(data=previous_resolved, resource_state=previous_state)
				config_bound = BoundState(data={}, resource_state=resource.null_state)

				task_session = self.create_task_session()
				input_ref = task_session.ns.new('input', previous_state.state.type)
				task_session.set_data('input', previous_resolved)

				output_ref = resource.plan(previous_bound, config_bound, task_session, input_ref)

				# In theory this should _not_ allow unknowns, but keeping it less strict for now.
				output_partial_resolved = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

				action = ExecuteTaskSession(
					task_session=task_session,
					task_input_key='input',
					output_key=node,
					output_ref=output_ref,
					config_state=resource.null_state,
					previous_state=previous_state,
					resource=resource,
					input_symbol=symbols.Literal(
						value=previous_resolved,
						type=previous_state.state.type,
						registry=config_session.ns.registry
					)
				)

				args.update({
					'current_data': previous_resolved,
					'current_type': previous_state.state.type,
					'current_state': previous_state,
					'current_action': action
				})
			elif previous_exists:
				args.update({
					'current_data': state_dep_graph.nodes[node]['value'],
					'current_type': state_dep_graph.nodes[node]['type'],
					'current_state': None,
					'current_action': DeleteValue(node),
					# If the old value is not stateful, there are no dependencies to deleting it.
					'current_depends_on': []
				})

			if config_state:
				resource = config_session.ns.registry.get_resource(config_state.resource_name)
				config_partial_resolved = config_session.resolve(config_session.ns.ref(node), decode=False, allow_unknowns=True)

				previous_bound = BoundState(data={}, resource_state=resource.null_state)
				config_bound = BoundState(data=config_partial_resolved, resource_state=config_state)

				task_session = self.create_task_session()
				input_ref = task_session.ns.new('input', config_state.state.type)
				task_session.set_data('input', config_partial_resolved)

				output_ref = resource.plan(previous_bound, config_bound, task_session, input_ref)

				# In theory this should _not_ allow unknowns, but keeping it less strict for now.
				output_partial_resolved = task_session.resolve(output_ref, allow_unknowns=True, decode=False)

				action = ExecuteTaskSession(
					task_session=task_session,
					task_input_key='input',
					output_key=node,
					output_ref=output_ref,
					config_state=config_state,
					previous_state=resource.null_state,
					resource=resource,
					input_symbol=config_session.ns.ref(node)
				)

				args.update({
					'config_data': output_partial_resolved,
					'config_type': config_state.state.type,
					'config_state': config_state,
					'config_action': action
				})
			elif config_exists:
				ref = config_session.ns.ref(node)
				action = SetValue(node, ref)

				args.update({
					'config_data': config_session.resolve(ref, decode=False, allow_unknowns=True),
					'config_type': config_session.ns.resolve(node),
					'config_state': None,
					'config_action': action
				})

			plan_node = PlanNode(**args)
			plan_nodes.append(plan_node)

		plan = Plan(tuple(plan_nodes), config_session, state_graph)
		# This will ensure that the task graph is a valid DAG, not perfect but it will
		# do for now
		plan.task_graph()
		return plan
