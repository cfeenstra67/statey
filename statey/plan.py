import abc
import asyncio
import contextlib
import dataclasses as dc
import sys
from functools import reduce, partial
from itertools import chain
from typing import Optional, Sequence, Dict, Tuple, Any

import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph

from statey import exc
from statey.provider import Provider, ProviderId
from statey.resource import (
    ResourceSession,
    BoundState,
    ResourceState,
    Resource,
    ResourceGraph,
    StateConfig,
    StateSnapshot,
)
from statey.syms import session, types, Object, stack, utils, impl
from statey.task import (
    TaskSession,
    SessionSwitch,
    SessionSwitchAction,
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
        state_session: session.Session,
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
    """"""

    task_session: TaskSession
    task_input_key: str
    task_state_key: str
    output_ref: Object
    graph_ref: Object
    output_key: str
    config_state: ResourceState
    previous_state: ResourceState
    resource: Resource
    input_symbol: Object
    state_symbol: Object
    state_allow_unknowns: bool = False

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
        state_session: session.Session,
        dependencies: Sequence[str],
    ) -> None:

        input_switch_task = SessionSwitch(
            [
                SessionSwitchAction(
                    input_session=output_session,
                    input_symbol=self.input_symbol,
                    output_session=self.task_session,
                    output_key=self.task_input_key,
                ),
                SessionSwitchAction(
                    input_session=state_session,
                    input_symbol=self.state_symbol,
                    output_session=self.task_session,
                    output_key=self.task_state_key,
                    allow_unknowns=self.state_allow_unknowns,
                ),
            ]
        )

        input_key = self.input_task(prefix)
        graph.add_node(input_key, task=input_switch_task, source=prefix)

        # If output state is null, our graph operation is a deletion. Otherwise, it's a "set"
        if self.config_state == self.resource.null_state:
            output_key = self.output_task(prefix)
            output_graph_task = GraphDeleteKey(
                key=self.output_key,
                resource_graph=resource_graph,
                input_session=self.task_session,
                input_symbol=self.graph_ref,
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
                [
                    SessionSwitchAction(
                        input_session=self.task_session,
                        input_symbol=self.output_ref,
                        output_session=output_session,
                        output_key=self.output_key,
                    )
                ]
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
        return f"{prefix}:state"

    def session_switch_task(self, prefix: str) -> str:
        return f"{prefix}:output"

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
        state_session: session.Session,
        dependencies: Sequence[str],
    ) -> None:

        graph_task_name = self.graph_update_task(prefix)
        if self.set_graph:
            graph_task = GraphSetKey(
                input_session=output_session,
                key=self.output_key,
                input_symbol=self.output_symbol,
                resource_graph=resource_graph,
                dependencies=dependencies,
            )
            graph.add_node(graph_task_name, task=graph_task, source=prefix)

        session_task_name = self.session_switch_task(prefix)
        if self.set_session:
            session_task = SessionSwitch(
                [
                    SessionSwitchAction(
                        input_session=output_session,
                        input_symbol=self.output_symbol,
                        output_session=output_session,
                        output_key=self.output_key,
                    )
                ]
            )
            graph.add_node(session_task_name, task=session_task, source=prefix)
            if self.set_graph:
                graph.add_edge(graph_task_name, session_task_name)


@dc.dataclass(frozen=True)
class ResourceSetValue(PlanAction):
    """
    SetValue for a resource, a no-op action
    """

    output_key: str
    output_symbol: Object
    resource: Resource
    state: ResourceState

    def graph_update_task(self, prefix: str) -> str:
        return f"{prefix}:state"

    def session_switch_task(self, prefix: str) -> str:
        return f"{prefix}:output"

    def input_task(self, prefix: str) -> str:
        return self.graph_update_task(prefix)

    def output_task(self, prefix: str) -> str:
        return self.session_switch_task(prefix)

    def render(
        self,
        graph: nx.DiGraph,
        prefix: str,
        resource_graph: ResourceGraph,
        output_session: session.Session,
        state_session: session.Session,
        dependencies: Sequence[str],
    ) -> None:

        session_task_name = self.session_switch_task(prefix)
        session_task = SessionSwitch(
            [
                SessionSwitchAction(
                    input_session=output_session,
                    input_symbol=self.output_symbol,
                    output_session=output_session,
                    output_key=self.output_key,
                )
            ]
        )
        graph.add_node(session_task_name, task=session_task, source=prefix)

        graph_task_name = self.graph_update_task(prefix)
        graph_task = GraphSetKey(
            input_session=output_session,
            key=self.output_key,
            input_symbol=self.output_symbol,
            resource_graph=resource_graph,
            dependencies=dependencies,
            state=self.state,
            finalize=self.resource.finalize,
        )
        graph.add_node(graph_task_name, task=graph_task, source=prefix)

        graph.add_edge(graph_task_name, session_task_name)


@dc.dataclass(frozen=True)
class DeleteValue(PlanAction):
    """"""

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
        state_session: session.Session,
        dependencies: Sequence[str],
    ) -> None:

        graph_task_name = self.input_task(prefix)
        graph_task = GraphDeleteKey(key=self.delete_key, resource_graph=resource_graph)
        graph.add_node(graph_task_name, task=graph_task, source=prefix)


@dc.dataclass(frozen=True)
class PlanNodeState:
    """
    Encapsulates information about one the key associated with a plan node, and
    the transition required to get there
    """

    data: Any
    type: types.Type
    state: Optional[ResourceState]
    depends_on: Sequence[str]
    action: PlanAction


@dc.dataclass(frozen=True)
class PlanNode:
    """
    A plan node contains information about the state of a given name and before
    and after this migration, including any unknowns. It also contains information
    about the up and downstream dependencies of the configuration and previous state
    respectively
    """

    key: str
    current: Optional[PlanNodeState] = None
    config: Optional[PlanNodeState] = None
    enforce_dependencies: bool = True

    def input_task(self) -> Optional[str]:
        """"""
        return self.current_input_task()

    def output_task(self) -> Optional[str]:
        """"""
        return self.config_output_task()

    def current_prefix(self) -> str:
        """"""
        if (
            self.current is not None
            and self.current.action is not None
            and self.config is not None
            and self.config.action is not None
        ):
            return f"{self.key}:current"
        return self.key

    def config_prefix(self) -> str:
        """"""
        if (
            self.current is not None
            and self.current.action is not None
            and self.config is not None
            and self.config.action is not None
        ):
            return f"{self.key}:config"
        return self.key

    def current_output_task(self) -> Optional[str]:
        """
        The output task of migrating the current state. This may or may not be the
        same as output_task(); the case where it is the same is when either this key
        only exists in one of the previous of configured namespaces or when it is a single
        resource whose state is being migrated.
        """
        action = (self.current and self.current.action) or (
            self.config and self.config.action
        )
        if action is None:
            return None
        return action.output_task(self.current_prefix())

    def config_output_task(self) -> Optional[str]:
        """"""
        action = (self.config and self.config.action) or (
            self.current and self.current.action
        )
        if action is None:
            return None
        return action.output_task(self.config_prefix())

    def config_input_task(self) -> str:
        """"""
        action = (self.config and self.config.action) or (
            self.current and self.current.action
        )
        if action is None:
            return None
        return action.input_task(self.config_prefix())

    def current_input_task(self) -> str:
        """"""
        action = (self.current and self.current.action) or (
            self.config and self.config.action
        )
        if action is None:
            return None
        return action.input_task(self.current_prefix())

    def get_edges(
        self, other_nodes: Dict[str, "PlanNode"]
    ) -> Sequence[Tuple[str, str]]:
        """
        Get the edges for this node, given the other nodes.
        """
        edges = {}

        if not self.enforce_dependencies:
            return []

        # input_task = self.input_task()
        current_input_task = self.config_input_task()
        if current_input_task is None:
            return []

        config_input_task = self.config_input_task()
        current_output_task = self.current_output_task()
        config_output_task = self.config_output_task()

        for upstream in self.config and self.config.depends_on or []:
            node = other_nodes[upstream]
            ref = node.output_task()
            if ref is None:
                continue
            edges[ref, config_input_task] = {"optional": False}

        for downstream in self.current and self.current.depends_on or []:
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

            check_set = (
                {(current_ref, current_input_task)}
                if self.current and self.current.action
                else {(config_ref, config_input_task)}
            )

            if not check_set & set(edges):
                input_ref = node.current_input_task()
                edges[current_output_task, input_ref] = {"optional": True}

        return [(*key, val) for key, val in edges.items()]

    def get_task_graph(
        self,
        resource_graph: ResourceGraph,
        output_session: session.Session,
        state_session: session.Session,
    ) -> nx.DiGraph:
        """
        Render a task graph for this specific node of the plan. The names specified
        by current_output_task() and output_task() should be tasks in this graph.
        """
        tasks = nx.DiGraph()

        if (
            self.current
            and self.current.action is not None
            and self.config
            and self.config.action is not None
        ):
            self.current.action.render(
                graph=tasks,
                prefix=self.current_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                state_session=state_session,
                # The current task will always have a null result
                dependencies=(),
            )
            self.config.action.render(
                graph=tasks,
                prefix=self.config_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                dependencies=self.config.depends_on,
                state_session=state_session,
            )
            # Make these two graphs depend on one another
            tasks.add_edge(self.current_output_task(), self.config_input_task())
        elif self.current and self.current.action:
            self.current.action.render(
                graph=tasks,
                prefix=self.current_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                state_session=state_session,
                dependencies=(),
            )
        elif self.config and self.config.action:
            self.config.action.render(
                graph=tasks,
                prefix=self.config_prefix(),
                resource_graph=resource_graph,
                output_session=output_session,
                state_session=state_session,
                dependencies=self.config.depends_on,
            )

        return tasks


@dc.dataclass(frozen=True)
class Plan:
    """
    A plan contains enough information to inspect and execute the sequence
    of tasks to reach the state of config_session from state_graph
    """

    nodes: Sequence[PlanNode]
    providers: Sequence[Provider]
    config_session: ResourceSession
    state_session: ResourceSession
    state_graph: ResourceGraph
    task_graph: "ResourceTaskGraph" = dc.field(init=False, default=None)

    def check_dag(self, graph: nx.DiGraph, strict: bool = False) -> None:
        """
        Check that this graph is a DAG, attempting to remove optional
        edges to remove conflicts if necessary
        """
        while not strict:
            try:
                cycle = nx.find_cycle(graph, orientation="reverse")
            except nx.NetworkXNoCycle:
                return

            removed = False

            for left, right, _ in cycle:
                edge = graph.edges[left, right]
                if edge.get("optional"):
                    graph.remove_edge(left, right)
                    removed = True
                    break

            if not removed:
                break

        # This will raise a properly formatted error
        utils.check_dag(dag)

    def new_task_graph(self, strict: bool = False) -> "ResourceTaskGraph":
        """
        Render a full task graph from this plan
        """
        from statey.executor import ResourceTaskGraph

        output_session = self.config_session.clone()

        node_dict = {node.key: node for node in self.nodes}

        graphs = []
        raw_edges = {}

        for node in self.nodes:
            graphs.append(
                node.get_task_graph(
                    resource_graph=self.state_graph,
                    output_session=output_session,
                    state_session=self.state_session,
                )
            )
            node_edges = node.get_edges(node_dict)
            for edge_from, edge_to, data in node_edges:
                raw_edges.setdefault((edge_from, edge_to), []).append(data)

        edges = [
            (*key, {"optional": all(item.get("optional") for item in vals)})
            for key, vals in raw_edges.items()
        ]

        # This will raise an error if there are overlapping keys, which there
        # shouldn't be.
        full_graph = reduce(nx.union, graphs) if graphs else nx.DiGraph()
        full_graph.add_edges_from(edges)

        self.check_dag(full_graph, strict)

        return ResourceTaskGraph(full_graph, output_session, self.state_graph)

    def build_task_graph(self, force: bool = False, **kwargs) -> None:
        """
        Create the task graph for this plan if it does not already exist
        """
        if self.task_graph is not None and not force:
            return

        self.__dict__["task_graph"] = self.new_task_graph(**kwargs)

    def is_empty(self, include_metatasks: bool = False) -> bool:
        """
        Indicate whether this plan is empty, meaning any tasks it contains
        are metatasks or there are no tasks at all
        """
        self.build_task_graph()
        for node in self.task_graph.task_graph.nodes:
            task = self.task_graph.get_task(node)
            if include_metatasks or not task.is_metatask():
                return False
        return True


class Migrator(abc.ABC):
    """
    Migrator is the interface for creating plans for migrating one set of resource states
    to another
    """

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

    @contextlib.contextmanager
    def error_ctx(self) -> None:
        with stack.rewrite_ctx():
            try:
                yield
            except Exception as err:
                raise exc.ErrorDuringPlanning(err, sys.exc_info()[2]) from err

    @contextlib.contextmanager
    def node_error_ctx(
        self, key: str, snapshot: StateSnapshot, config: StateConfig
    ) -> None:
        with stack.rewrite_ctx():
            try:
                yield
            except Exception as err:
                raise exc.ErrorDuringPlanningNode(
                    key, snapshot, config, err, sys.exc_info()[2]
                ) from err

    def create_task_session(
        self, data: Optional[Dict[str, Object]] = None
    ) -> TaskSession:
        if data is None:
            data = {}
        session = create_task_session()
        for key, val in data.items():
            session.set(key, val)
        return session

    async def get_or_create_provider(
        self,
        id: ProviderId,
        providers: Dict[ProviderId, Provider],
        registry: "Registry",
    ) -> Provider:
        """"""
        provider = providers.get(id)
        if provider is None:
            provider = providers[id] = registry.get_provider(*id)
            await provider.setup()

        return provider

    async def plan_node_migrate_states(
        self,
        node: str,
        config_session: ResourceSession,
        state_session: ResourceSession,
        config_dep_graph: nx.DiGraph,
        state_dep_graph: nx.DiGraph,
        output_session: ResourceSession,
        providers: Dict[ProviderId, Provider],
    ) -> Sequence[PlanNode]:
        """"""
        config_bound_state = output_session.get_state(node)
        config_state = config_bound_state.state
        previous_state = state_session.get_state(node).state

        current_depends_on = list(state_dep_graph.pred[node])
        config_depends_on = list(config_dep_graph.pred[node])

        provider = await self.get_or_create_provider(
            config_state.provider, providers, config_session.ns.registry
        )

        resource = provider.get_resource(config_state.resource)

        previous_ref = state_session.ns.ref(node)
        previous_resolved = state_session.resolve(
            previous_ref, decode=False, allow_unknowns=True
        )
        config_partial_resolved = output_session.resolve(
            config_bound_state.input, decode=False, allow_unknowns=True
        )

        task_session = self.create_task_session(
            {
                "input": Object[config_state.input_type](config_partial_resolved),
                "state": Object[previous_state.output_type](previous_resolved),
            }
        )

        input_ref = task_session.ns.ref("input")
        task_state_ref = task_session.ns.ref("state")

        previous_snapshot = StateSnapshot(task_state_ref, previous_state)
        bound_config = StateConfig(input_ref, config_state)

        current_action = None
        state_symbol = previous_ref

        with self.node_error_ctx(node, previous_snapshot, bound_config):

            try:
                plan_output = await resource.plan(
                    previous_snapshot, bound_config, task_session
                )
            except exc.NullRequired:
                null_bound_config = StateConfig({}, resource.null_state)

                old_task_session = self.create_task_session(
                    {
                        "input": Object[resource.null_state.input_type]({}),
                        "state": Object[previous_state.output_type](previous_resolved),
                    }
                )
                old_state_ref = old_task_session.ns.ref("state")

                old_previous_snapshot = StateSnapshot(old_state_ref, previous_state)

                # old_task_session = task_session_base.clone()
                og_plan_output = await resource.plan(
                    old_previous_snapshot, null_bound_config, old_task_session
                )

                if isinstance(og_plan_output, tuple) and len(og_plan_output) == 2:
                    og_output_ref, og_graph_ref = og_plan_output
                else:
                    og_output_ref, og_graph_ref = og_plan_output, og_plan_output

                null_snapshot = StateSnapshot({}, resource.null_state)

                task_session = self.create_task_session(
                    {
                        "input": Object[config_state.input_type](
                            config_partial_resolved
                        ),
                        "state": Object[resource.null_state.output_type]({}),
                    }
                )

                plan_output = await resource.plan(
                    null_snapshot, bound_config, task_session
                )

                current_action = ExecuteTaskSession(
                    task_session=old_task_session,
                    task_input_key="input",
                    task_state_key="state",
                    output_key=node,
                    output_ref=og_output_ref,
                    graph_ref=og_graph_ref,
                    config_state=resource.null_state,
                    previous_state=previous_state,
                    resource=resource,
                    input_symbol=null_snapshot.obj,
                    state_symbol=previous_ref,
                )
                state_symbol = Object({})

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
        # This indicates there are no tasks in the session.
        if not test_graph.nodes:
            try:
                resolved_graph_ref = task_session.resolve(graph_ref, decode=False)
            # The graph reference isn't fully resolved yet, we still need
            # to execute the task session
            except exc.UnknownError:
                pass
            else:
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
                    current=PlanNodeState(
                        data=previous_resolved,
                        type=previous_state.state.output_type,
                        state=previous_state,
                        depends_on=current_depends_on,
                        action=None,
                    ),
                    config=PlanNodeState(
                        data=resolved_graph_ref,
                        type=config_state.state.output_type,
                        state=config_state,
                        depends_on=config_depends_on,
                        action=ResourceSetValue(
                            node, state_obj, resource, config_state
                        ),
                    ),
                    enforce_dependencies=False,
                )
                return [plan_node]

        action = ExecuteTaskSession(
            task_session=task_session,
            task_input_key="input",
            task_state_key="state",
            output_key=node,
            output_ref=output_ref,
            graph_ref=graph_ref,
            config_state=config_state,
            previous_state=previous_state,
            resource=resource,
            input_symbol=config_bound_state.input,
            state_symbol=state_symbol,
        )

        plan_node = PlanNode(
            key=node,
            current=PlanNodeState(
                data=previous_resolved,
                type=previous_state.state.output_type,
                state=previous_state,
                depends_on=current_depends_on,
                action=current_action,
            ),
            config=PlanNodeState(
                data=output_partial_resolved,
                type=config_state.state.output_type,
                state=config_state,
                depends_on=config_depends_on,
                action=action,
            ),
        )
        return [plan_node]

    async def plan_node_previous(
        self,
        node: str,
        config_session: ResourceSession,
        state_session: ResourceSession,
        config_dep_graph: nx.DiGraph,
        state_dep_graph: nx.DiGraph,
        output_session: ResourceSession,
        providers: Dict[ProviderId, Provider],
    ) -> Sequence[PlanNode]:

        pass

    async def plan_node_config(
        self,
        node: str,
        config_session: ResourceSession,
        state_session: ResourceSession,
        config_dep_graph: nx.DiGraph,
        state_dep_graph: nx.DiGraph,
        output_session: ResourceSession,
        providers: Dict[ProviderId, Provider],
    ) -> Sequence[PlanNode]:

        pass

    async def plan_node(
        self,
        node: str,
        config_session: ResourceSession,
        state_session: ResourceSession,
        config_dep_graph: nx.DiGraph,
        state_dep_graph: nx.DiGraph,
        output_session: ResourceSession,
        providers: Dict[ProviderId, Provider],
    ) -> Sequence[PlanNode]:
        """"""
        previous_exists = node in state_dep_graph.nodes
        previous_state = None
        previous_bound_state = None
        try:
            previous_bound_state = state_session.get_state(node)
        except exc.SymbolKeyError:
            pass
        else:
            previous_state = previous_bound_state.state

        config_exists = node in config_dep_graph.nodes
        config_bound_state = None
        config_state = None
        try:
            config_bound_state = output_session.get_state(node)
        except exc.SymbolKeyError:
            pass
        else:
            config_state = config_bound_state.state

        current_depends_on = list(state_dep_graph.pred[node]) if previous_exists else []
        config_depends_on = list(config_dep_graph.pred[node]) if config_exists else []

        plan_nodes = []

        # We can handle this node with one action--otherwise we need two, though
        # one or both may just be state update operations.
        if (
            config_state is not None
            and previous_state is not None
            and config_state.resource == previous_state.resource
        ):
            return await self.plan_node_migrate_states(
                node=node,
                config_session=config_session,
                state_session=state_session,
                config_dep_graph=config_dep_graph,
                state_dep_graph=state_dep_graph,
                output_session=output_session,
                providers=providers,
            )

        current_node_state = None
        config_node_state = None

        if previous_state:
            provider = await self.get_or_create_provider(
                previous_state.provider, providers, state_session.ns.registry
            )

            resource = provider.get_resource(previous_state.resource)

            task_session = self.create_task_session()

            previous_ref = state_session.ns.ref(node)
            previous_resolved = state_session.resolve(
                previous_ref, decode=False, allow_unknowns=True
            )

            task_session = self.create_task_session(
                {
                    "state": Object[previous_state.output_type](previous_resolved),
                    "input": Object[resource.null_state.input_type]({}),
                }
            )

            state_ref = task_session.ns.ref("state")
            previous_snapshot = StateSnapshot(state_ref, previous_state)

            input_ref = task_session.ns.ref("input")
            config_bound = StateConfig(input_ref, resource.null_state)

            with self.error_ctx():
                plan_output = await resource.plan(
                    previous_snapshot, config_bound, task_session
                )

            # Allow different refs for graph vs. session by returning a tuple of refs
            if isinstance(plan_output, tuple) and len(plan_output) == 2:
                output_ref, graph_ref = plan_output
            else:
                output_ref, graph_ref = plan_output, plan_output

            with self.error_ctx():
                # In theory this should _not_ allow unknowns, but keeping it less strict for now.
                # Update: removed unknowns, they should not be needed
                # Update2: These are needed for now
                output_resolved = task_session.resolve(
                    output_ref, decode=False, allow_unknowns=True
                )

            action = ExecuteTaskSession(
                task_session=task_session,
                task_input_key="input",
                task_state_key="state",
                output_key=node,
                output_ref=output_ref,
                graph_ref=graph_ref,
                config_state=resource.null_state,
                previous_state=previous_state,
                resource=resource,
                input_symbol=Object({}, types.EmptyType()),
                state_symbol=previous_ref,
            )

            current_node_state = PlanNodeState(
                data=previous_resolved,
                type=previous_state.output_type,
                state=previous_state,
                action=action,
                depends_on=current_depends_on,
            )
        elif previous_exists and not config_exists:
            current_node_state = PlanNodeState(
                data=state_dep_graph.nodes[node]["value"],
                type=state_dep_graph.nodes[node]["type"],
                state=None,
                action=DeleteValue(node),
                # If the old value is not stateful, there are no dependencies to deleting it.
                depends_on=[],
            )

        if config_state:
            provider = providers.get(config_state.provider)
            if provider is None:
                provider = providers[
                    config_state.provider
                ] = config_session.ns.registry.get_provider(*config_state.provider)
                await provider.setup()

            resource = provider.get_resource(config_state.resource)
            config_partial_resolved = output_session.resolve(
                config_bound_state.input, decode=False, allow_unknowns=True
            )

            task_session = self.create_task_session(
                {
                    "state": Object[resource.null_state.output_type]({}),
                    "input": Object[config_state.state.input_type](
                        config_partial_resolved
                    ),
                }
            )

            state_ref = task_session.ns.ref("state")
            previous_snapshot = StateSnapshot(state_ref, resource.null_state)

            input_ref = task_session.ns.ref("input")
            config_bound = StateConfig(input_ref, config_state)

            with self.error_ctx():
                plan_output = await resource.plan(
                    previous_snapshot, config_bound, task_session
                )

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
                task_state_key="state",
                output_key=node,
                output_ref=output_ref,
                graph_ref=graph_ref,
                config_state=config_state,
                previous_state=resource.null_state,
                resource=resource,
                input_symbol=config_bound_state.input,
                state_symbol=Object({}),
            )

            config_node_state = PlanNodeState(
                data=output_partial_resolved,
                type=config_state.state.output_type,
                state=config_state,
                action=action,
                depends_on=config_depends_on,
            )
        elif config_exists:
            ref = config_session.ns.ref(node)

            config_node_state = PlanNodeState(
                data=config_session.resolve(ref, decode=False, allow_unknowns=True),
                type=config_session.ns.resolve(node),
                state=None,
                action=SetValue(node, ref),
                depends_on=config_depends_on,
            )

        plan_node = PlanNode(node, current_node_state, config_node_state)
        plan_nodes.append(plan_node)

        return plan_nodes

    async def plan(
        self,
        config_session: ResourceSession,
        state_graph: Optional[ResourceGraph] = None,
        state_session: Optional[ResourceSession] = None,
    ) -> Plan:

        from statey.executor import AsyncIOTaskGraph, AsyncIOGraphExecutor

        if state_graph is None:
            state_graph = ResourceGraph()

        if state_session is None:
            state_session = state_graph.to_session()

        with self.error_ctx():
            config_dep_graph = config_session.dependency_graph()

        with self.error_ctx():
            state_dep_graph = state_session.dependency_graph()

        # For planning we need a copy of the config session that we will partially resolve
        # (maybe w/ unknowns) as we go
        output_session = config_session.clone()

        config_nodes = set(config_dep_graph.nodes)
        state_nodes = set(state_dep_graph.nodes)

        state_only_nodes = state_nodes - config_nodes

        plan_nodes = []
        providers = {}

        task_graph = nx.DiGraph()

        async def coro(node):
            plan_nodes.extend(
                await self.plan_node(
                    node=node,
                    config_session=config_session,
                    state_session=state_session,
                    config_dep_graph=config_dep_graph,
                    state_dep_graph=state_dep_graph,
                    output_session=output_session,
                    providers=providers,
                )
            )

        for node in chain(config_dep_graph.nodes, state_only_nodes):
            task_graph.add_node(node, coroutine_factory=partial(coro, node))

        task_graph.add_edges_from(((a, b) for a, b, _ in config_dep_graph.edges))
        task_graph_obj = AsyncIOTaskGraph(task_graph)
        executor = AsyncIOGraphExecutor()

        try:
            exec_info = await executor.execute_async(task_graph_obj)
        finally:
            providers = tuple(providers.values())
            await asyncio.gather(*(provider.teardown() for provider in providers))

        with self.error_ctx():
            exec_info.raise_for_failure()

        plan = Plan(
            tuple(plan_nodes), providers, config_session, state_session, state_graph
        )
        # This will ensure that the task graph is a valid DAG, not perfect but it will
        # do for now
        plan.build_task_graph()
        return plan
