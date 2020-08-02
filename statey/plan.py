import abc
import dataclasses as dc
import sys
from functools import reduce
from itertools import chain
from typing import Optional, Sequence, Dict, Tuple, Any

import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph

from statey import exc
from statey.resource import (
    ResourceSession,
    BoundState,
    ResourceState,
    Resource,
    ResourceGraph,
    StateConfig,
    StateSnapshot,
)
from statey.syms import session, types, Object, stack, utils
from statey.task import (
    TaskSession,
    SessionSwitch,
    GraphDeleteKey,
    GraphSetKey,
    create_task_session,
)


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
        dependencies: Sequence[str],
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
    output_ref: Object
    graph_ref: Object
    output_key: str
    config_state: ResourceState
    previous_state: ResourceState
    resource: Resource
    input_symbol: Object

    def input_task(self, prefix: str) -> str:
        return f"{prefix}:input"

    def output_task(self, prefix: str) -> str:
        return f"{prefix}:output"

    def render(
        self,
        graph: nx.DiGraph,
        prefix: str,
        resource_graph: ResourceGraph,
        output_session: session.Session,
        # Dependencies of the configuration
        dependencies: Sequence[str],
    ) -> None:

        input_switch_task = SessionSwitch(
            input_session=output_session,
            input_symbol=self.input_symbol,
            output_session=self.task_session,
            output_key=self.task_input_key,
        )

        input_key = self.input_task(prefix)
        graph.add_node(input_key, task=input_switch_task, source=prefix)

        # If output state is null, our graph operation is a deletion. Otherwise, it's a "set"
        if self.config_state == self.resource.s.null_state:
            output_key = self.output_task(prefix)
            output_graph_task = GraphDeleteKey(
                key=self.output_key, resource_graph=resource_graph
            )
            graph.add_node(output_key, task=output_graph_task, source=prefix)
        else:
            output_key = f"{prefix}:state"
            output_graph_task = GraphSetKey(
                input_session=self.task_session,
                input_symbol=self.graph_ref,
                dependencies=dependencies,
                key=self.output_key,
                resource_graph=resource_graph,
                state=self.config_state,
                finalize=self.resource.finalize,
            )
            graph.add_node(output_key, task=output_graph_task, source=prefix)

            output_switch_task = SessionSwitch(
                input_session=self.task_session,
                input_symbol=self.output_ref,
                output_session=output_session,
                output_key=self.output_key,
            )
            output_switch_key = self.output_task(prefix)
            graph.add_node(output_switch_key, task=output_switch_task, source=prefix)

            # We should always update the state before the session
            graph.add_edge(output_key, output_switch_key)

        task_graph = self.task_session.task_graph(resource_graph, self.output_key)
        if not task_graph.nodes:
            graph.add_edge(input_key, output_key)

        task_key = lambda x: f"{prefix}:task:{x}"

        for sub_node in topological_sort(task_graph):
            node_key = task_key(sub_node)
            task = task_graph.nodes[sub_node]["task"]

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
	Set a given value in either the resource graph, the output session, or both during execution
	"""

    output_key: str
    output_symbol: Object
    set_graph: bool = True
    set_session: bool = True

    def __post_init__(self) -> None:
        if not (self.set_graph or self.set_session):
            raise ValueError(
                "At least one of `set_session` or `set_graph` must be true."
            )

    def graph_update_task(self, prefix: str) -> str:
        return f"{prefix}:graph-update"

    def session_switch_task(self, prefix: str) -> str:
        return f"{prefix}:session-switch"

    def input_task(self, prefix: str) -> str:
        if self.set_graph and self.set_session or not self.set_session:
            return self.graph_update_task(prefix)
        return self.session_switch_task(prefix)

    def output_task(self, prefix: str) -> str:
        if self.set_session:
            return self.session_switch_task(prefix)
        return self.graph_update_task(prefix)

    def render(
        self,
        graph: nx.DiGraph,
        prefix: str,
        resource_graph: ResourceGraph,
        output_session: session.Session,
        # Dependencies of the configuration
        dependencies: Sequence[str],
    ) -> None:

        input_task_name = self.input_task(prefix)
        if self.set_graph:
            graph_task = GraphSetKey(
                input_session=output_session,
                key=self.output_key,
                input_symbol=self.output_symbol,
                resource_graph=resource_graph,
                dependencies=dependencies,
            )
            graph.add_node(input_task_name, task=graph_task, source=prefix)

        output_task_name = self.output_task(prefix)
        if self.set_session:
            session_task = SessionSwitch(
                input_session=output_session,
                input_symbol=self.output_symbol,
                output_session=output_session,
                output_key=self.output_key,
            )
            graph.add_node(output_task_name, task=session_task, source=prefix)
            if self.set_graph:
                graph.add_edge(input_task_name, output_task_name)


@dc.dataclass(frozen=True)
class DeleteValue(PlanAction):
    """

	"""

    delete_key: str

    def input_task(self, prefix: str) -> str:
        return f"{prefix}:state"

    def output_task(self, prefix: str) -> str:
        return f"{prefix}:state"

    def render(
        self,
        graph: nx.DiGraph,
        prefix: str,
        resource_graph: ResourceGraph,
        output_session: session.Session,
        # Dependencies of the configuration
        dependencies: Sequence[str],
    ) -> None:

        graph_task_name = self.input_task(prefix)
        graph_task = GraphDeleteKey(key=self.delete_key, resource_graph=resource_graph)
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

    def input_task(self) -> Optional[str]:
        """

		"""
        return self.current_input_task()

    def output_task(self) -> Optional[str]:
        """

        """
        return self.config_output_task()

    def current_prefix(self) -> str:
        """

		"""
        if self.current_action is not None and self.config_action is not None:
            return f"{self.key}:current"
        return self.key

    def config_prefix(self) -> str:
        """

        """
        if self.current_action is not None and self.config_action is not None:
            return f"{self.key}:config"
        return self.key

    def current_output_task(self) -> Optional[str]:
        """
		The output task of migrating the current state. This may or may not be the
		same as output_task(); the case where it is the same is when either this key
		only exists in one of the previous of configured namespaces or when it is a single
		resource whose state is being migrated.
		"""
        action = self.current_action or self.config_action
        if action is None:
            return None
        return action.output_task(self.current_prefix())

    def config_output_task(self) -> Optional[str]:
        """

        """
        action = self.config_action or self.current_action
        if action is None:
            return None
        return action.output_task(self.config_prefix())

    def config_input_task(self) -> str:
        """

		"""
        action = self.config_action or self.current_action
        if action is None:
            return None
        return action.input_task(self.config_prefix())

    def current_input_task(self) -> str:
        """

        """
        action = self.current_action or self.config_action
        if action is None:
            return None
        return action.input_task(self.current_prefix())

    def get_edges(
        self, other_nodes: Dict[str, "PlanNode"]
    ) -> Sequence[Tuple[str, str]]:
        """
		Get the edges for this node, given the other nodes.
		"""
        edges = set()

        # input_task = self.input_task()
        current_input_task = self.config_input_task()
        if current_input_task is None:
            return edges

        config_input_task = self.config_input_task()
        current_output_task = self.current_output_task()
        config_output_task = self.config_output_task()

        for upstream in self.config_depends_on:
            node = other_nodes[upstream]
            ref = node.output_task()
            if ref is None:
                continue
            edges.add((ref, config_input_task))

        for downstream in self.current_depends_on:
            node = other_nodes[downstream]
            current_ref = node.current_output_task()
            config_ref = node.config_output_task()
            if config_ref is None:
                continue
            # So we are checking if our input/config currently depends on the _current_
            # output of the other task. If not, we'll make the other input dependent
            # on the _current output task_
            # if not {(current_ref, current_input_task)} & edges:
            #     input_ref = node.current_input_task()
            #     edges.add((current_output_task, input_ref))

            # if not {(config_ref, config_input_task), (current_ref, )} & edges:
            #     input_ref = node.config_input_task()
            #     edges.add((config_output_task, input_ref))

            check_set = {(current_ref, current_input_task)} if self.current_action else {(config_ref, config_input_task)}

            if not check_set & edges:
                input_ref = node.current_input_task()
                edges.add((current_output_task, input_ref))

        return edges

    def get_task_graph(
        self, resource_graph: ResourceGraph, output_session: session.Session
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
                dependencies=(),
            )
            self.config_action.render(
                graph=tasks,
                prefix=self.config_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                dependencies=self.config_depends_on,
            )
            # Make these two graphs depend on one another
            tasks.add_edge(self.current_output_task(), self.config_input_task())
        elif self.current_action:
            self.current_action.render(
                graph=tasks,
                prefix=self.current_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                dependencies=(),
            )
        elif self.config_action:
            self.config_action.render(
                graph=tasks,
                prefix=self.config_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                dependencies=self.config_depends_on,
            )

        return tasks


@dc.dataclass(frozen=True)
class Plan:
    nodes: Sequence[PlanNode]
    config_session: ResourceSession
    state_graph: ResourceGraph

    def task_graph(self) -> "TaskGraph":
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
            graphs.append(
                node.get_task_graph(
                    resource_graph=state_graph, output_session=output_session
                )
            )
            edges |= node.get_edges(node_dict)

        # This will raise an error if there are overlapping keys, which there
        # shouldn't be.
        full_graph = reduce(nx.union, graphs) if graphs else nx.DiGraph()
        full_graph.add_edges_from(edges)

        utils.check_dag(full_graph)

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
    async def plan(
        self,
        config_session: ResourceSession,
        state_session: Optional[ResourceSession] = None,
    ) -> Plan:
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
        return create_task_session()

    # async def resource_homo_plan_node(
    #     self,
    #     node: str,
    #     resource: Resource,
    #     config_session: ResourceSession,
    #     previous_bound: StateSnapshot,
    #     config_bound_state: BoundState,
    #     current_depends_on: Sequence[str],
    #     config_depends_on: Sequence[str]
    # ) -> PlanNode:
    #     """

    #     """
    #     config_state = None
    #     if config_bound_state is None:
    #         config_state = resource.s.null_state
    #         config_bound_state = BoundState(config_state, {})
    #     else:
    #         config_state = config_bound_state.state

    #     config_partial_resolved = config_session.resolve(
    #         config_bound_state.input, decode=False, allow_unknowns=True
    #     )

    #     previous_state = None
    #     if previous_bound is None:
    #         previous_state = resource.s.null_state
    #         previous_bound = StateSnapshot({}, previous_state)
    #     else:
    #         previous_state = previous_bound.state

    #     task_session_base = self.create_task_session()
    #     input_ref = task_session_base.ns.new("input", config_state.state.input_type)
    #     task_session_base.set_data("input", config_partial_resolved)

    #     task_session = task_session_base.clone()

    #     bound_config = StateConfig(input_ref, config_state)

    #     # A bunch of junk to handle errors nicely
    #     error, tb = None, None
    #     current_action = None
    #     try:
    #         plan_output = await resource.plan(
    #             previous_bound, bound_config, task_session
    #         )
    #     except exc.NullRequired:
    #         null_bound_config = StateConfig({}, resource.s.null_state)
    #         null_bound_snapshot = StateSnapshot({}, resource.s.null_state)

    #         current_task_session = task_session_base.clone()
    #         task_session = task_session_base.clone()

    #         first_plan_output = await resource.plan(
    #             previous_bound, null_bound_config, current_task_session
    #         )
    #         # Allow different refs for graph vs. session by returning a tuple of refs
    #         if isinstance(first_plan_output, tuple) and len(first_plan_output) == 2:
    #             first_output_ref, first_graph_ref = first_plan_output
    #         else:
    #             first_output_ref, first_graph_ref = first_plan_output, first_plan_output

    #         plan_output = await resource.plan(
    #             null_bound_snapshot, bound_config, task_session
    #         )
    #         # Allow different refs for graph vs. session by returning a tuple of refs
    #         if isinstance(first_plan_output, tuple) and len(first_plan_output) == 2:
    #             output_ref, graph_ref = plan_output
    #         else:
    #             output_ref, graph_ref = plan_output, plan_output

    #         current_action = ExecuteTaskSession(
    #             task_session=current_task_session,
    #             task_input_key="input",
    #             output_key=node,
    #             output_ref=first_output_ref,
    #             graph_ref=first_graph_ref,
    #             config_state=resource.s.null_state,
    #             previous_state=previous_state,
    #             resource=resource,
    #             input_symbol=Object({}, resource.s.null_state.input_type, config_session.ns.registry)
    #         )
    #         previous_bound = null_bound_snapshot
    #         previous_state = previous_bound.state
    #     except Exception as err:
    #         error = exc.ErrorDuringPlanningNode(
    #             node, previous_bound, bound_config, err
    #         )
    #         _, _, tb = sys.exc_info()
    #     else:
    #         # Allow different refs for graph vs. session by returning a tuple of refs
    #         if isinstance(plan_output, tuple) and len(plan_output) == 2:
    #             output_ref, graph_ref = plan_output
    #         else:
    #             output_ref, graph_ref = plan_output, plan_output

    #     if error:
    #         try:
    #             raise error.with_traceback(tb)
    #         except Exception:
    #             stack.rewrite_tb(*sys.exc_info())
    #     # End error junk

    #     output_partial_resolved = task_session.resolve(
    #         output_ref, allow_unknowns=True, decode=False
    #     )
    #     config_session.set_data(node, output_partial_resolved)

    #     test_graph = task_session.task_graph(ResourceGraph(), node)
    #     print("NODE", node)
    #     print("HERE", previous_state)
    #     print("HERE2", config_state)
    #     print("FUC", graph_ref)
    #     print("GRAPH", test_graph.nodes)
    #     # This indicates there are no tasks in the session and the state has not changed.
    #     if (
    #         not test_graph.nodes
    #         and previous_state.state.name == config_state.state.name
    #         and previous_state.state.type == config_state.state.type
    #         and current_depends_on == config_depends_on
    #     ):
    #         try:
    #             resolved_graph_ref = task_session.resolve(
    #                 graph_ref, decode=False
    #             )
    #         # The graph reference isn't fully resolved yet, we still need
    #         # to execute the task session
    #         except exc.UnknownError:
    #             pass
    #         else:
    #             if resolved_graph_ref == previous_bound.data:
    #                 state_obj = Object(
    #                     resolved_graph_ref,
    #                     config_state.state.output_type,
    #                     config_session.ns.registry,
    #                 )
    #                 # Since we are not setting this key in the graph, we can
    #                 # act like the configuration has no dependencies during
    #                 # planning. This essentially lets us avoid circular
    #                 # dependencies because these no-op resources will never
    #                 # depend on anything.
    #                 plan_node = PlanNode(
    #                     key=node,
    #                     current_data=previous_bound.data,
    #                     current_type=previous_state.state.output_type,
    #                     current_state=previous_state,
    #                     current_depends_on=(),
    #                     current_action=None,
    #                     config_data=resolved_graph_ref,
    #                     config_type=config_state.state.output_type,
    #                     config_state=config_state,
    #                     config_depends_on=(),
    #                     config_action=SetValue(
    #                         node, state_obj, set_graph=False
    #                     ),
    #                 )
    #                 return plan_node

    #     action = ExecuteTaskSession(
    #         task_session=task_session,
    #         task_input_key="input",
    #         output_key=node,
    #         output_ref=output_ref,
    #         graph_ref=graph_ref,
    #         config_state=config_state,
    #         previous_state=previous_state,
    #         resource=resource,
    #         input_symbol=config_bound_state.input,
    #     )

    #     return PlanNode(
    #         key=node,
    #         current_data=previous_bound.data,
    #         current_type=previous_state.state.output_type,
    #         current_state=previous_state,
    #         current_depends_on=current_depends_on,
    #         current_action=current_action,
    #         config_data=output_partial_resolved,
    #         config_type=config_state.state.output_type,
    #         config_state=config_state,
    #         config_depends_on=config_depends_on,
    #         config_action=action,
    #     )

    # async def plan_node(
    #     self,
    #     node: str,
    #     config_session: ResourceSession,
    #     config_dep_graph: nx.DiGraph,
    #     state_graph: ResourceGraph
    # ) -> PlanNode:
    #     """
    #     Plan for a single node
    #     """
    #     state_dep_graph = state_graph.graph

    #     previous_exists = node in state_dep_graph
    #     previous_state = None
    #     if previous_exists:
    #         previous_state = state_dep_graph.nodes[node]['state']

    #     config_exists = node in config_dep_graph
    #     config_bound_state = None
    #     config_state = None
    #     try:
    #         config_bound_state = config_session.get_state(node)
    #     except exc.SymbolKeyError:
    #         pass
    #     else:
    #         config_state = config_bound_state.state

    #     current_depends_on = (
    #         list(state_dep_graph.pred[node]) if previous_exists else []
    #     )
    #     config_depends_on = (
    #         list(config_dep_graph.pred[node]) if config_exists else []
    #     )

    #     # We can handle this node with one action--otherwise we need two, though
    #     # one or both may just be state update operations.
    #     if (
    #         config_state is not None
    #         and previous_state is not None
    #         and config_state.resource == previous_state.resource
    #     ):
    #         resource = config_session.ns.registry.get_resource(
    #             config_state.resource
    #         )
    #         previous_resolved = state_dep_graph.nodes[node]["value"]
    #         previous_snapshot = StateSnapshot(previous_resolved, previous_state)

    #         return await self.resource_homo_plan_node(
    #             node=node,
    #             resource=resource,
    #             config_session=config_session,
    #             previous_bound=previous_snapshot,
    #             config_bound_state=config_bound_state,
    #             current_depends_on=current_depends_on,
    #             config_depends_on=config_depends_on
    #         )

    #     args = {
    #         "key": node,
    #         "current_data": None,
    #         "current_type": None,
    #         "current_state": None,
    #         "current_depends_on": current_depends_on,
    #         "current_action": None,
    #         "config_data": None,
    #         "config_type": None,
    #         "config_state": None,
    #         "config_depends_on": config_depends_on,
    #         "config_action": None,
    #     }

    #     if previous_state:
    #         resource = config_session.ns.registry.get_resource(
    #             previous_state.resource
    #         )
    #         previous_resolved = state_dep_graph.nodes[node]["value"]

    #         previous_snapshot = StateSnapshot(previous_resolved, previous_state)

    #         null_state = resource.s.null_state

    #         task_session = self.create_task_session()
    #         input_ref = task_session.ns.new("input", null_state.input_type)

    #         config_bound = StateConfig(input_ref, null_state)
    #         task_session.set_data("input", {})

    #         # A bunch of junk to handle errors nicely
    #         error, tb = None, None
    #         try:
    #             plan_output = await resource.plan(
    #                 previous_snapshot, config_bound, task_session
    #             )
    #         except Exception as err:
    #             error = exc.ErrorDuringPlanningNode(
    #                 node, previous_snapshot, config_bound, err
    #             )
    #             _, _, tb = sys.exc_info()
    #         if error:
    #             try:
    #                 raise error.with_traceback(tb)
    #             except Exception:
    #                 stack.rewrite_tb(*sys.exc_info())
    #         # End error junk

    #         # Allow different refs for graph vs. session by returning a tuple of refs
    #         if isinstance(plan_output, tuple) and len(plan_output) == 2:
    #             output_ref, graph_ref = plan_output
    #         else:
    #             output_ref, graph_ref = plan_output, plan_output

    #         error, tb = None, None
    #         try:
    #             # In theory this should _not_ allow unknowns, but keeping it less strict for now.
    #             # Update: removed unknowns, they should not be needed
    #             # Update2: These are needed for now
    #             output_resolved = task_session.resolve(
    #                 output_ref, decode=False, allow_unknowns=True
    #             )
    #         except Exception as err:
    #             error = exc.ErrorDuringPlanningNode(
    #                 node, previous_snapshot, config_bound, err
    #             )
    #             _, _, tb = sys.exc_info()
    #         if error:
    #             try:
    #                 raise error.with_traceback(tb)
    #             except Exception:
    #                 stack.rewrite_tb(*sys.exc_info())

    #         action = ExecuteTaskSession(
    #             task_session=task_session,
    #             task_input_key="input",
    #             output_key=node,
    #             output_ref=output_ref,
    #             graph_ref=graph_ref,
    #             config_state=resource.s.null_state,
    #             previous_state=previous_state,
    #             resource=resource,
    #             input_symbol=Object(
    #                 {}, config_bound.type, task_session.ns.registry
    #             ),
    #         )

    #         args.update(
    #             {
    #                 "current_data": previous_resolved,
    #                 "current_type": previous_state.output_type,
    #                 "current_state": previous_state,
    #                 "current_action": action,
    #             }
    #         )
    #     elif previous_exists:
    #         args.update(
    #             {
    #                 "current_data": state_dep_graph.nodes[node]["value"],
    #                 "current_type": state_dep_graph.nodes[node]["type"],
    #                 "current_state": None,
    #                 "current_action": DeleteValue(node),
    #                 # If the old value is not stateful, there are no dependencies to deleting it.
    #                 "current_depends_on": [],
    #             }
    #         )

    #     if config_state:
    #         resource = config_session.ns.registry.get_resource(
    #             config_state.resource
    #         )
    #         config_partial_resolved = config_session.resolve(
    #             config_bound_state.input, decode=False, allow_unknowns=True
    #         )

    #         previous_snapshot = StateSnapshot({}, resource.s.null_state)

    #         task_session = self.create_task_session()
    #         input_ref = task_session.ns.new("input", config_state.state.input_type)
    #         task_session.set_data("input", config_partial_resolved)

    #         config_bound = StateConfig(input_ref, config_state)

    #         # A bunch of junk to handle errors nicely
    #         error, tb = None, None
    #         try:
    #             plan_output = await resource.plan(
    #                 previous_snapshot, config_bound, task_session
    #             )
    #         except Exception as err:
    #             error = exc.ErrorDuringPlanningNode(
    #                 node, previous_snapshot, config_bound, err
    #             )
    #             _, _, tb = sys.exc_info()
    #         if error:
    #             try:
    #                 raise error.with_traceback(tb)
    #             except Exception:
    #                 stack.rewrite_tb(*sys.exc_info())
    #         # End error junk
    #         # Allow different refs for graph vs. session by returning a tuple of refs
    #         if isinstance(plan_output, tuple) and len(plan_output) == 2:
    #             output_ref, graph_ref = plan_output
    #         else:
    #             output_ref, graph_ref = plan_output, plan_output

    #         output_partial_resolved = task_session.resolve(
    #             output_ref, allow_unknowns=True, decode=False
    #         )

    #         config_session.set_data(node, output_partial_resolved)

    #         action = ExecuteTaskSession(
    #             task_session=task_session,
    #             task_input_key="input",
    #             output_key=node,
    #             output_ref=output_ref,
    #             graph_ref=graph_ref,
    #             config_state=config_state,
    #             previous_state=resource.s.null_state,
    #             resource=resource,
    #             input_symbol=config_bound_state.input,
    #         )

    #         args.update(
    #             {
    #                 "config_data": output_partial_resolved,
    #                 "config_type": config_state.state.output_type,
    #                 "config_state": config_state,
    #                 "config_action": action,
    #             }
    #         )
    #     elif config_exists:
    #         args.update({
    #             "config_data": config_session.resolve(
    #                 ref, decode=False, allow_unknowns=True
    #             ),
    #             "config_data": config_session.ns.resolve(node),
    #             "config_state": None,
    #             "config_action": action
    #         })

    #     return PlanNode(**args)

    # async def plan(
    #     self,
    #     config_session: ResourceSession,
    #     state_graph: Optional[ResourceGraph] = None,
    # ) -> Plan:

    #     # Ugly error handling :(
    #     error, tb = None, None
    #     try:
    #         config_dep_graph = config_session.dependency_graph()
    #     except Exception as err:
    #         error = exc.ErrorDuringPlanning(err)
    #         _, _, tb = sys.exc_info()
    #     if error:
    #         try:
    #             raise error.with_traceback(tb)
    #         except Exception:
    #             stack.rewrite_tb(*sys.exc_info())

    #     # For planning we need a copy of the config session that we will partially resolve
    #     # (maybe w/ unknowns) as we go
    #     output_session = config_session.clone()

    #     config_nodes = set(config_dep_graph.nodes)
    #     state_nodes = set(state_graph.graph.nodes)

    #     state_only_nodes = state_nodes - config_nodes

    #     plan_nodes = []

    #     for node in chain(topological_sort(config_dep_graph), state_only_nodes):
    #         plan_nodes.append(await self.plan_node(
    #             node=node,
    #             config_session=output_session,
    #             config_dep_graph=config_dep_graph,
    #             state_graph=state_graph
    #         ))
    #         # error, tb = None, None
    #         # try:

    #         # except Exception as err:
    #         #     snapshot = 
    #         #     error = exc.ErrorDuringPlanning(
    #         #         node, pre
    #         #     )


    #     plan = Plan(tuple(plan_nodes), config_session, state_graph)
    #     # This will ensure that the task graph is a valid DAG, not perfect but it will
    #     # do for now
    #     plan.task_graph()
    #     return plan

    async def plan(
        self,
        config_session: ResourceSession,
        state_graph: Optional[ResourceGraph] = None,
    ) -> Plan:

        # Ugly error handling :(
        error, tb = None, None
        try:
            config_dep_graph = config_session.dependency_graph()
        except Exception as err:
            error = exc.ErrorDuringPlanning(err)
            _, _, tb = sys.exc_info()
        if error:
            try:
                raise error.with_traceback(tb)
            except Exception:
                stack.rewrite_tb(*sys.exc_info())

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
                previous_state = state_dep_graph.nodes[node]["state"]

            config_exists = node in config_nodes
            config_bound_state = None
            config_state = None
            try:
                config_bound_state = output_session.get_state(node)
            except exc.SymbolKeyError:
                pass
            else:
                config_state = config_bound_state.state

            current_depends_on = (
                list(state_dep_graph.pred[node]) if previous_exists else []
            )
            config_depends_on = (
                list(config_dep_graph.pred[node]) if config_exists else []
            )

            # We can handle this node with one action--otherwise we need two, though
            # one or both may just be state update operations.
            if (
                config_state is not None
                and previous_state is not None
                and config_state.resource == previous_state.resource
            ):
                resource = output_session.ns.registry.get_resource(
                    config_state.resource
                )
                previous_resolved = state_dep_graph.nodes[node]["value"]

                config_partial_resolved = output_session.resolve(
                    config_bound_state.input, decode=False, allow_unknowns=True
                )
                previous_snapshot = StateSnapshot(previous_resolved, previous_state)

                task_session_base = self.create_task_session()
                input_ref = task_session_base.ns.new("input", config_state.state.input_type)
                task_session_base.set_data("input", config_partial_resolved)

                task_session = task_session_base.clone()

                bound_config = StateConfig(input_ref, config_state)

                # A bunch of junk to handle errors nicely
                error, tb = None, None
                current_action = None
                try:
                    # plan_output = await resource.plan(
                    #     previous_snapshot, bound_config, task_session
                    # )

                    try:
                        plan_output = await resource.plan(
                            previous_snapshot, bound_config, task_session
                        )
                    except exc.NullRequired:
                        null_bound_config = StateConfig({}, resource.s.null_state)

                        old_task_session = self.create_task_session()
                        old_task_session.ns.new("input", resource.s.null_state.input_type)
                        old_task_session.set_data("input", {})
                        # old_task_session = task_session_base.clone()
                        og_plan_output = await resource.plan(
                            previous_snapshot, null_bound_config, old_task_session
                        )

                        if isinstance(og_plan_output, tuple) and len(og_plan_output) == 2:
                            og_output_ref, og_graph_ref = og_plan_output
                        else:
                            og_output_ref, og_graph_ref = og_plan_output, og_plan_output

                        null_snapshot = StateSnapshot({}, resource.s.null_state)
                        task_session = task_session_base.clone()
                        plan_output = await resource.plan(
                            null_snapshot, bound_config, task_session
                        )

                        current_action = ExecuteTaskSession(
                            task_session=old_task_session,
                            task_input_key="input",
                            output_key=node,
                            output_ref=og_output_ref,
                            graph_ref=og_graph_ref,
                            config_state=resource.s.null_state,
                            previous_state=previous_state,
                            resource=resource,
                            input_symbol=null_snapshot.obj
                        )

                except Exception as err:
                    error = exc.ErrorDuringPlanningNode(
                        node, previous_snapshot, bound_config, err
                    )
                    _, _, tb = sys.exc_info()
                if error:
                    try:
                        raise error.with_traceback(tb)
                    except Exception:
                        stack.rewrite_tb(*sys.exc_info())
                # End error junk

                # Allow different refs for graph vs. session by returning a tuple of refs
                if isinstance(plan_output, tuple) and len(plan_output) == 2:
                    output_ref, graph_ref = plan_output
                else:
                    output_ref, graph_ref = plan_output, plan_output

                output_partial_resolved = task_session.resolve(
                    output_ref, allow_unknowns=True, decode=False
                )
                output_session.set_data(node, output_partial_resolved)

                test_graph = task_session.task_graph(ResourceGraph(), node)
                # This indicates there are no tasks in the session and the state has not changed.
                if (
                    not test_graph.nodes
                    and previous_state.state.name == config_state.state.name
                    and previous_state.state.type == config_state.state.type
                    and current_depends_on == config_depends_on
                ):
                    try:
                        resolved_graph_ref = task_session.resolve(
                            graph_ref, decode=False
                        )
                    # The graph reference isn't fully resolved yet, we still need
                    # to execute the task session
                    except exc.UnknownError:
                        pass
                    else:
                        if resolved_graph_ref == previous_resolved:
                            state_obj = Object(
                                resolved_graph_ref,
                                config_state.state.output_type,
                                config_session.ns.registry,
                            )
                            # Since we are not setting this key in the graph, we can
                            # act like the configuration has no dependencies during
                            # planning. This essentially lets us avoid circular
                            # dependencies because these no-op resources will never
                            # depend on anything.
                            plan_node = PlanNode(
                                key=node,
                                current_data=previous_resolved,
                                current_type=previous_state.state.output_type,
                                current_state=previous_state,
                                current_depends_on=(),
                                current_action=None,
                                config_data=resolved_graph_ref,
                                config_type=config_state.state.output_type,
                                config_state=config_state,
                                config_depends_on=(),
                                config_action=SetValue(
                                    node, state_obj, set_graph=False
                                ),
                            )
                            plan_nodes.append(plan_node)
                            continue

                action = ExecuteTaskSession(
                    task_session=task_session,
                    task_input_key="input",
                    output_key=node,
                    output_ref=output_ref,
                    graph_ref=graph_ref,
                    config_state=config_state,
                    previous_state=previous_state,
                    resource=resource,
                    input_symbol=config_bound_state.input,
                )

                plan_node = PlanNode(
                    key=node,
                    current_data=previous_resolved,
                    current_type=previous_state.state.output_type,
                    current_state=previous_state,
                    current_depends_on=current_depends_on,
                    current_action=current_action,
                    config_data=output_partial_resolved,
                    config_type=config_state.state.output_type,
                    config_state=config_state,
                    config_depends_on=config_depends_on,
                    config_action=action,
                )
                plan_nodes.append(plan_node)
                continue

            args = {
                "key": node,
                "current_data": None,
                "current_type": None,
                "current_state": None,
                "current_depends_on": current_depends_on,
                "current_action": None,
                "config_data": None,
                "config_type": None,
                "config_state": None,
                "config_depends_on": config_depends_on,
                "config_action": None,
            }

            if previous_state:
                resource = config_session.ns.registry.get_resource(
                    previous_state.resource
                )
                previous_resolved = state_dep_graph.nodes[node]["value"]

                previous_snapshot = StateSnapshot(previous_resolved, previous_state)

                null_state = resource.s.null_state

                task_session = self.create_task_session()
                input_ref = task_session.ns.new("input", null_state.input_type)

                config_bound = StateConfig(input_ref, null_state)
                task_session.set_data("input", {})

                # A bunch of junk to handle errors nicely
                error, tb = None, None
                try:
                    plan_output = await resource.plan(
                        previous_snapshot, config_bound, task_session
                    )
                except Exception as err:
                    error = exc.ErrorDuringPlanningNode(
                        node, previous_snapshot, config_bound, err
                    )
                    _, _, tb = sys.exc_info()
                if error:
                    try:
                        raise error.with_traceback(tb)
                    except Exception:
                        stack.rewrite_tb(*sys.exc_info())
                # End error junk

                # Allow different refs for graph vs. session by returning a tuple of refs
                if isinstance(plan_output, tuple) and len(plan_output) == 2:
                    output_ref, graph_ref = plan_output
                else:
                    output_ref, graph_ref = plan_output, plan_output

                error, tb = None, None
                try:
                    # In theory this should _not_ allow unknowns, but keeping it less strict for now.
                    # Update: removed unknowns, they should not be needed
                    # Update2: These are needed for now
                    output_resolved = task_session.resolve(
                        output_ref, decode=False, allow_unknowns=True
                    )
                except Exception as err:
                    error = exc.ErrorDuringPlanningNode(
                        node, previous_snapshot, config_bound, err
                    )
                    _, _, tb = sys.exc_info()
                if error:
                    try:
                        raise error.with_traceback(tb)
                    except Exception:
                        stack.rewrite_tb(*sys.exc_info())

                action = ExecuteTaskSession(
                    task_session=task_session,
                    task_input_key="input",
                    output_key=node,
                    output_ref=output_ref,
                    graph_ref=graph_ref,
                    config_state=resource.s.null_state,
                    previous_state=previous_state,
                    resource=resource,
                    input_symbol=Object(
                        {}, config_bound.type, task_session.ns.registry
                    ),
                )

                args.update(
                    {
                        "current_data": previous_resolved,
                        "current_type": previous_state.output_type,
                        "current_state": previous_state,
                        "current_action": action,
                    }
                )
            elif previous_exists:
                args.update(
                    {
                        "current_data": state_dep_graph.nodes[node]["value"],
                        "current_type": state_dep_graph.nodes[node]["type"],
                        "current_state": None,
                        "current_action": DeleteValue(node),
                        # If the old value is not stateful, there are no dependencies to deleting it.
                        "current_depends_on": [],
                    }
                )

            if config_state:
                resource = config_session.ns.registry.get_resource(
                    config_state.resource
                )
                config_partial_resolved = output_session.resolve(
                    config_bound_state.input, decode=False, allow_unknowns=True
                )

                previous_snapshot = StateSnapshot({}, resource.s.null_state)

                task_session = self.create_task_session()
                input_ref = task_session.ns.new("input", config_state.state.input_type)
                task_session.set_data("input", config_partial_resolved)

                config_bound = StateConfig(input_ref, config_state)

                # A bunch of junk to handle errors nicely
                error, tb = None, None
                try:
                    plan_output = await resource.plan(
                        previous_snapshot, config_bound, task_session
                    )
                except Exception as err:
                    error = exc.ErrorDuringPlanningNode(
                        node, previous_snapshot, config_bound, err
                    )
                    _, _, tb = sys.exc_info()
                if error:
                    try:
                        raise error.with_traceback(tb)
                    except Exception:
                        stack.rewrite_tb(*sys.exc_info())
                # End error junk
                # Allow different refs for graph vs. session by returning a tuple of refs
                if isinstance(plan_output, tuple) and len(plan_output) == 2:
                    output_ref, graph_ref = plan_output
                else:
                    output_ref, graph_ref = plan_output, plan_output

                output_partial_resolved = task_session.resolve(
                    output_ref, allow_unknowns=True, decode=False
                )

                output_session.set_data(node, output_partial_resolved)

                action = ExecuteTaskSession(
                    task_session=task_session,
                    task_input_key="input",
                    output_key=node,
                    output_ref=output_ref,
                    graph_ref=graph_ref,
                    config_state=config_state,
                    previous_state=resource.s.null_state,
                    resource=resource,
                    input_symbol=config_bound_state.input,
                )

                args.update(
                    {
                        "config_data": output_partial_resolved,
                        "config_type": config_state.state.output_type,
                        "config_state": config_state,
                        "config_action": action,
                    }
                )
            elif config_exists:
                ref = config_session.ns.ref(node)
                action = SetValue(node, ref)

                args.update(
                    {
                        "config_data": config_session.resolve(
                            ref, decode=False, allow_unknowns=True
                        ),
                        "config_type": config_session.ns.resolve(node),
                        "config_state": None,
                        "config_action": action,
                    }
                )

            plan_node = PlanNode(**args)
            plan_nodes.append(plan_node)

        plan = Plan(tuple(plan_nodes), config_session, state_graph)
        # This will ensure that the task graph is a valid DAG, not perfect but it will
        # do for now
        plan.task_graph()
        return plan
