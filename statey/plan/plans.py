import abc
import dataclasses as dc
from functools import reduce
from itertools import product
from typing import Sequence, Optional, Dict, Tuple, Any, Callable

import networkx as nx

from statey.plan.actions import PlanAction
from statey.provider import Provider
from statey.resource import ResourceState, ResourceSession, ResourceGraph
from statey.syms import types, session


def check_dag(graph: nx.DiGraph, strict: bool = False) -> None:
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


class Plan(abc.ABC):
    """"""

    task_graph: "ResourceTaskGraph"

    @abc.abstractmethod
    def build_task_graph(self) -> None:
        """
        Call to build and validate the task grpah for this plan. After this is called,
        `self.task_graph` should not be None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def is_empty(self, include_metatasks: bool = False) -> bool:
        """
        Indicate whether this plan is empty, meaning any tasks it contains
        are metatasks or there are no tasks at all
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def plan(
        self, config_session: ResourceSession, migrator: Optional["Migrator"] = None
    ) -> "Plan":
        """
        Create a plan from the output of this plan and a new session
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class DefaultPlan(Plan):
    """
    A plan contains enough information to inspect and execute the sequence
    of tasks to reach the state of config_session from state_graph
    """

    nodes: Sequence[PlanNode]
    providers: Sequence[Provider]
    config_session: ResourceSession
    state_session: ResourceSession
    state_graph: ResourceGraph
    migrator: Optional["Migrator"] = None
    task_graph: "ResourceTaskGraph" = dc.field(init=False, default=None)

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

        check_dag(full_graph, strict)

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

    async def plan(
        self, config_session: ResourceSession, migrator: Optional["Migrator"] = None
    ) -> "Plan":
        """
        Create a new plan from the output of this plan to the new `config_session`.
        """
        if migrator is None:
            migrator = self.migrator

        if migrator is None:
            migrator = DefaultMigrator()

        self.build_task_graph()
        new_plan = await migrator.plan(
            config_session, self.state_graph, self.task_graph.output_session
        )
        compound = CompoundPlan(self, new_plan)
        compound.build_task_graph()
        return compound


@dc.dataclass(frozen=True)
class CompoundPlan(Plan):
    """"""

    first: Plan
    second: Plan
    task_graph: "ResourceTaskGraph" = dc.field(init=False, default=None)

    def _prefix_graph(self, key: Callable[[str], str], graph: nx.DiGraph) -> nx.DiGraph:

        new_graph = nx.DiGraph()

        for node in graph.nodes:
            new_graph.add_node(key(node), **graph.nodes[node])

        translated_edges = [
            (key(left), key(right), graph.edges[left, right])
            for left, right in graph.edges
        ]

        new_graph.add_edges_from(translated_edges)
        return new_graph

    def new_task_graph(self, strict: bool = False) -> "ResourceTaskGraph":
        """
        Render a full task graph from this plan
        """
        from statey.executor import ResourceTaskGraph

        self.first.build_task_graph()
        self.second.build_task_graph()

        graph = nx.DiGraph()

        first_graph = self.first.task_graph.task_graph
        second_graph = self.second.task_graph.task_graph

        first_key = "first:{}".format
        second_key = "second:{}".format

        first_prefixed_graph = self._prefix_graph(first_key, first_graph)
        second_prefixed_graph = self._prefix_graph(second_key, second_graph)

        graph = nx.union(first_prefixed_graph, second_prefixed_graph)
        graph.add_edges_from(first_prefixed_graph.edges)
        graph.add_edges_from(second_prefixed_graph.edges)

        first_end_tasks = {
            node
            for node in first_prefixed_graph.nodes
            if not first_prefixed_graph.succ[node]
        }
        second_start_tasks = {
            node
            for node in second_prefixed_graph.nodes
            if not second_prefixed_graph.pred[node]
        }

        edges = []

        for first_task, second_task in product(first_end_tasks, second_start_tasks):
            edges.append((first_task, second_task, {"optional": False}))

        graph.add_edges_from(edges)

        check_dag(graph, strict)

        return ResourceTaskGraph(
            task_graph=graph,
            output_session=self.second.task_graph.output_session,
            resource_graph=self.second.task_graph.resource_graph,
        )

    def is_empty(self, include_metatasks: bool = False) -> bool:
        return self.first.is_empty() and self.second.is_empty()

    # Same build_task_graph() and plan() behavior as DefaultPlan
    build_task_graph = DefaultPlan.build_task_graph

    plan = DefaultPlan.plan
