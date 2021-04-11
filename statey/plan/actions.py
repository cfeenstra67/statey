import abc
import dataclasses as dc
from typing import Sequence

import networkx as nx

import statey as st
from statey.resource import ResourceGraph, ResourceState, Resource
from statey.syms import session, Object
from statey.task import (
    TaskSession,
    SessionSwitch,
    SessionSwitchAction,
    GraphSetKey,
    GraphDeleteKey,
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

        for sub_node in nx.topological_sort(task_graph):
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
        return None
        # return self.graph_update_task(prefix)

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
