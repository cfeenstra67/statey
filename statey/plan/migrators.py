import abc
import asyncio
import contextlib
import sys
from functools import partial
from itertools import chain
from typing import Sequence, Optional, Dict

import networkx as nx

import statey as st
from statey import exc
from statey.plan.actions import (
    ExecuteTaskSession,
    SetValue,
    DeleteValue,
    ResourceSetValue,
)
from statey.plan.plans import Plan, DefaultPlan, PlanNode, PlanNodeState, StatefulPlan
from statey.provider import Provider, ProviderId
from statey.resource import (
    ResourceSession,
    ResourceGraph,
    StateConfig,
    StateSnapshot,
    DefaultResourceGraph,
)
from statey.syms import Object, stack, types
from statey.task import TaskSession, create_task_session


class Migrator(abc.ABC):
    """
    Migrator is the interface for creating plans for migrating one set of resource states
    to another
    """

    @abc.abstractmethod
    async def plan(
        self,
        config_session: ResourceSession,
        state_graph: Optional[ResourceGraph] = None,
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

        test_graph = task_session.task_graph(DefaultResourceGraph(), node)
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

    # async def plan_node_previous(
    #     self,
    #     node: str,
    #     config_session: ResourceSession,
    #     state_session: ResourceSession,
    #     config_dep_graph: nx.DiGraph,
    #     state_dep_graph: nx.DiGraph,
    #     output_session: ResourceSession,
    #     providers: Dict[ProviderId, Provider],
    # ) -> Sequence[PlanNode]:

    #     pass

    # async def plan_node_config(
    #     self,
    #     node: str,
    #     config_session: ResourceSession,
    #     state_session: ResourceSession,
    #     config_dep_graph: nx.DiGraph,
    #     state_dep_graph: nx.DiGraph,
    #     output_session: ResourceSession,
    #     providers: Dict[ProviderId, Provider],
    # ) -> Sequence[PlanNode]:

    #     pass

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
            state_graph = DefaultResourceGraph()

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
            await asyncio.gather(
                *(provider.teardown() for provider in providers.values())
            )

        with self.error_ctx():
            exec_info.raise_for_failure()

        config_out = config_session.clone()

        # Before creating the plan, iterate through the output session and set
        # each value as an expectation for the config session
        for key in output_session.ns.keys():
            ref = output_session.ns.ref(key)
            value = output_session.resolve(ref, allow_unknowns=True, decode=False)
            current_value = output_session.get_data(key)
            typ = output_session.ns.resolve(key)
            obj = Object(current_value, typ) >> value
            config_out.set_data(key, obj)

        plan = DefaultPlan(
            nodes=tuple(plan_nodes),
            providers=providers,
            config_session=config_out,
            state_session=state_session,
            state_graph=state_graph,
            migrator=self,
        )
        # This will ensure that the task graph is a valid DAG, not perfect but it will
        # do for now
        plan.build_task_graph()
        return plan


class StatefulMigrator(DefaultMigrator):
    """
    Migrator that wraps the plan in a StatefulPlan and accepts an addition "state" keyword
    argument to the plan() method.
    """

    async def plan(
        self,
        config_session: ResourceSession,
        state_graph: Optional[ResourceGraph] = None,
        state_session: Optional[ResourceSession] = None,
        *,
        state: str
    ) -> Plan:

        plan = await super().plan(config_session, state_graph, state_session)
        return StatefulPlan(plan, state, self)
