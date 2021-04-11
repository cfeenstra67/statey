# import abc
# import dataclasses as dc
# from itertools import product
# from typing import Optional, Callable, Any, Dict, Sequence

# import networkx as nx

# import statey as st
# from statey.plan import Plan, StatefulMigrator
# from statey.resource import ResourceGraph, DefaultResourceGraph
# from statey.syms import session, types, utils


# class StatefulResourceGraph(ResourceGraph):
#     """
#     Resource graph that exposes two additional methods--get_ and set_ methods for the state
#     """

#     @abc.abstractmethod
#     def get_state(self) -> str:
#         """
#         Get the state name for this resource graph
#         """
#         raise NotImplementedError

#     @abc.abstractmethod
#     def set_state(self, state: str) -> None:
#         """
#         Set the state name for this resource graph
#         """
#         raise NotImplementedError


# @dc.dataclass(frozen=True)
# class DefaultStatefulResourceGraph(StatefulResourceGraph):
#     """
#     Default implementation for a stateful resource graph
#     """

#     graph: ResourceGraph
#     state: dc.InitVar[str] = "DOWN"
#     _state: str = dc.field(init=False, default=None)

#     def __post_init__(self, state: str) -> None:
#         self.set_state(state)

#     def get_state(self) -> str:
#         return self._state

#     def set_state(self, state: str) -> None:
#         self.__dict__["_state"] = state

#     def get(self, key: str) -> Dict[str, Any]:
#         return self.graph.get(key)

#     def keys(self) -> Sequence[str]:
#         return self.graph.keys()

#     def add_dependencies(self, key: str, dependencies: Sequence[str]) -> None:
#         self.graph.add_dependencies(key, dependencies)

#     async def refresh(
#         self, registry: Optional[st.Registry] = None, finalize: bool = False
#     ) -> Iterator[str]:
#         async for item in self.graph.refresh(registry, finalize):
#             yield item

#     def clone(self) -> "ResourceGraph":
#         return type(self)(graph=self.graph.clone(), state=self.get_state())

#     def to_dict(self, registry: Optional["Registry"] = None) -> Dict[str, Any]:
#         return {"state": self.get_state(), "resources": self.graph.to_dict(registry)}

#     @classmethod
#     def from_dict(
#         cls, data: Any, registry: Optional["Registry"] = None
#     ) -> "ResourceGraph":
#         graph = DefaultResourceGraph.from_dict(data["resources"], registry)
#         return cls(graph=graph, state=data["state"])

#     def to_session(self) -> ResourceSession:
#         return self.graph.to_session()


# class ModuleTransition:
#     """"""

#     func: Callable[[session.Session], None]
#     to_state: str
#     from_state: Optional[str] = None

#     def _get_session(self, states: Dict[str, types.Type]) -> session.Session:
#         """"""
#         session = st.create_resource_session()
#         if self.from_state is not None:
#             snapshot_type = states[self.from_state]
#             session["_snapshot"] << st.Unknown[snapshot_type]

#         self.func(session)
#         return session.ns.root._type

#     def session(self, states: Dict[str, types.Type]) -> session.Session:
#         """"""
#         return self._get_session(states)


# def build_state_graph(
#     transitions: Sequence[ModuleTransition], registry: Optional["Registry"] = None
# ) -> nx.DiGraph:
#     """"""
#     if registry is None:
#         registry = st.registry

#     graph = nx.DiGraph()
#     edges = set()
#     from_any_state = set()

#     # First just build the graph nodes, then add edges
#     for transition in transitions:
#         if transition.to_state in graph.nodes:
#             graph.nodes[transition.to_state]["transitions"].append(transition)
#         else:
#             graph.add_node(transition.to_state, transitions=[transition])
#         if transition.from_state is not None:
#             edges.add((transition.from_state, transition.to_state))
#         else:
#             from_any_state.add(transition.to_state)

#     graph.add_edges_from(edges)

#     states = {}

#     for node in nx.topological_sort(graph):
#         node_transitions = graph.nodes[node]["transitions"]

#         stable_output_type = None
#         stateful_transitions = []

#         for transition in node_transitions:
#             if transition.from_state is None:
#                 stable_output_type = transition.session(states).ns.root._type
#             else:
#                 stateful_transitions.append(transition)

#         if stable_output_type is None:
#             raise ValueError(f"{node} does not have a stable transition!")

#         invalid_transitions = []
#         for transition in stateful_transitions:
#             output_type = transition.session(states).ns.root._type
#             try:
#                 registry.get_caster(output_type, stable_output_type)
#             except st.exc.NoCasterFound:
#                 invalid_transitions.append(transition)

#         if invalid_transitions:
#             raise ValueError(
#                 f"{node}: transitions have invalid output types: {invalid_transitions}."
#             )

#         states[node] = graph.nodes[node]["type"] = stable_output_type

#     for from_node, to_node in product(graph.nodes, from_any_state):
#         if from_node == to_node:
#             continue
#         graph.add_edge(from_node, to_node)

#     return graph


# class Module(abc.ABC):
#     """"""

#     def __init__(self, transitions: Sequence[ModuleTransition]) -> None:
#         self.transitions = transitions
#         self.graph = build_state_graph(transitions)

#     def migrator(self) -> StatefulMigrator:
#         return StatefulMigrator()

#     async def plan(self, resource_graph: StatefulResourceGraph, desired: str) -> Plan:

#         current_state = resource_graph.get_state()

#         path = [current_state] + nx.shortest_path(current_state, desired, self.graph)

#         # First collect a list of transitions detailing the path
#         transitions = []
#         transition_steps = []

#         for from_state, to_state in zip(path, path[1:]):
#             node_transitions = self.graph.node[to_state]["transitions"]

#             added = False
#             stable = None

#             for transition in node_transitions:
#                 if transition.from_state == from_state:
#                     transitions.append(transition)
#                     added = True
#                     break
#                 elif transition.from_state is None:
#                     stable = transition

#             if not added:
#                 transitions.append(stable)

#             transition_steps.append(to_state)

#         head, *tail = transitions
#         head_state, *tail_states = transition_steps

#         migrator = self.migrator()

#         states = {node: self.graph.nodes[node]["type"] for node in self.graph.nodes}

#         head_session = head.session(states)

#         plan = await migrator.plan(head_session, resource_graph, state=head_state)

#         for transition, tail_state in zip(tail, tail_states):
#             session = transition.session(states)
#             plan = await plan.plan(session, state=tail_state)

#         return plan
