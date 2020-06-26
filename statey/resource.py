import abc
import asyncio
import dataclasses as dc
from collections import Counter
from typing import (
    Optional,
    Dict,
    Any,
    Sequence,
    Type as PyType,
    Tuple,
    Iterator,
)

import marshmallow as ma
import networkx as nx

import statey as st
from statey import exc
from statey.task import TaskSession
from statey.syms import types, utils, session, symbols


class StateSchema(ma.Schema):
    """
	Marshmallow schema for extra safety encoding/decoding states
	"""

    name = ma.fields.Str(required=True, default=None)
    type = ma.fields.Dict(required=True, default=None)
    null = ma.fields.Bool(required=True, default=None)


class AbstractState(abc.ABC):
    """
    Abstract base class defining required attributes for states
    """

    name: str
    type: types.Type
    null: bool

    def to_dict(self, registry: "Registry") -> Dict[str, Any]:
        """
        Render this state to a JSON-serializable dictionary
        """
        type_serializer = registry.get_type_serializer(self.type)
        out = {
            "name": self.name,
            "type": type_serializer.serialize(self.type),
            "null": self.null,
        }
        return StateSchema().dump(out)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], registry: "Registry") -> "State":
        """
        Render a State from the output of to_dict()
        """
        data = StateSchema().load(data)
        type_serializer = registry.get_type_serializer_from_data(data["type"])
        typ = type_serializer.deserialize(data["type"])
        return cls(name=data["name"], type=typ, null=data["null"])


@dc.dataclass(frozen=True)
class State(AbstractState):
    """
	A state corresponds to some type for a resource.
	"""

    name: str
    type: types.Type
    null: bool = dc.field(repr=False, default=False)


@dc.dataclass(frozen=True)
class NullState(State):
    """
	Null states must always have types.EmptyType as their type, so
	this is a helper to create such states.
	"""

    type: types.Type = dc.field(init=False, default=types.EmptyType)
    null: bool = dc.field(repr=False, init=False, default=True)


class ResourceStateSchema(ma.Schema):
    """
	Marshmallow schema for extra safety encoding/decoding resource states
	"""

    state = ma.fields.Nested(StateSchema(), required=True, default=None)
    resource_name = ma.fields.Str(required=True, default=None)


@dc.dataclass(frozen=True)
class ResourceState:
    """
	A resource state is a state that is bound to some resource.
	"""

    state: AbstractState
    resource_name: str

    def __call__(self, arg=utils.MISSING, **kwargs) -> "BoundState":
        """
		Factory method for creating bound states for this resource state.
		"""
        if arg is utils.MISSING:
            arg = kwargs
        elif kwargs:
            raise ValueError(
                "Either one positional arg or keyword arguments are required for a BoundState."
            )
        return BoundState(self, arg)

    def to_dict(self, registry: "Registry") -> Dict[str, Any]:
        """
		Render this resource state to a JSON-serializable dictionary
		"""
        out = {
            "resource_name": self.resource_name,
            "state": self.state.to_dict(registry),
        }
        return ResourceStateSchema().dump(out)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], registry: "Registry") -> "ResourceState":
        """
		Render a ResourceState from the output of to_dict()
		"""
        data = ResourceStateSchema().load(data)
        state = State.from_dict(data["state"], registry)
        return cls(resource_name=data["resource_name"], state=state)


@dc.dataclass(frozen=True)
class BoundState(utils.Cloneable):
    """
	A bound state binds a resource state to some date.
	"""

    resource_state: ResourceState
    data: Any

    def literal(self, registry: "Registry") -> symbols.Literal:
        """
		Convenience method to convert a bound state to a literal with some registry.
		"""
        semantics = registry.get_semantics(self.resource_state.state.type)
        return symbols.Literal(value=self.data, semantics=semantics)


class States(abc.ABC):
    """
	An interface for accessing states of a resource. While only a `null_state` implementation
	is required, additional methods can be exposed here to simplify resource state creation.
	"""
    resource_name: str

    @property
    @abc.abstractmethod
    def null_state(self) -> ResourceState:
        """
		Return the null state of this resource, at which point it will removed from
		a resource graph
		"""
        raise NotImplementedError


# class KnownStatesMeta(abc.ABCMeta):
#     """
# 	Metaclass for resources
# 	"""

#     @classmethod
#     def _validate_states(
#         cls, old_states: Sequence[State], new_states: Sequence[State]
#     ) -> Tuple[Sequence[State], State]:

#         new_names = Counter(state.name for state in new_states)
#         if new_names and max(new_names.values()) > 1:
#             multi = {k: v for k, v in new_names.items() if v > 1}
#             raise ValueError(f"Duplicate states found: {multi}")

#         old_states = [state for state in old_states if state.name not in new_names]
#         return old_states + list(new_states)

#     def __new__(
#         cls, name: str, bases: Sequence[PyType], attrs: Dict[str, Any]
#     ) -> PyType:
#         super_cls = super().__new__(cls, name, bases, attrs)
#         states = super_cls.__states__ if hasattr(super_cls, "__states__") else ()
#         new_states = [val for val in attrs.values() if isinstance(val, State)]
#         states = cls._validate_states(states, new_states)
#         super_cls.__states__ = tuple(states)
#         return super_cls


# class KnownStates(States, metaclass=KnownStatesMeta):
#     """
# 	Define a fixed set of known states
# 	"""

#     def __init__(self, resource_name: str) -> None:
#         self.resource_name = resource_name
#         # This is temporary, should clean this up
#         for state in self.__states__:
#             self.set_resource_state(ResourceState(state, resource_name))

#     def set_resource_state(self, state: ResourceState) -> None:
#         setattr(self, state.state.name, state)

#     @property
#     def null_state(self) -> ResourceState:
#         state = next((s for s in self.__states__ if s.null))
#         return ResourceState(state, self.resource_name)

#     def __call__(self, *args, **kwargs) -> ResourceState:
#         states = [state for state in self.__states__ if state != self.null_state.state]
#         if len(states) > 1:
#             raise TypeError(f'"{self.resource_name}" has more than one non-null state.')
#         return ResourceState(states[0], self.resource_name)(*args, **kwargs)


class Resource(abc.ABC):
    """
	A resource represents a stateful object of some kind, and it can have one
	or more "states" that that object can exist in.
	"""

    name: str
    States: PyType[States]

    def __init__(self) -> None:
        self._s = self.States(self.name)

    @property
    def s(self) -> States:
        """
		Returns information about the possible states of this resource
		"""
        return self._s

    @abc.abstractmethod
    def plan(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:
        """
		Given a task session, the current state of a resource, and a task session with
		corresponding input reference, return an output reference that can be fully
		resolved when all the tasks in the task session have been complete successfully.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    async def refresh(self, current: BoundState) -> BoundState:
        """
		Given the current bound state, return a new bound state that represents that actual
		current state of the object
		"""
        raise NotImplementedError

    async def finalize(self, data: BoundState) -> BoundState:
        """
        Called on states before they are committed to the resource graph. By default a nooop
        """
        return data


class ResourceSession(session.Session):
    """
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""

    def __init__(self, session: session.Session) -> None:
        super().__init__(session.ns)
        self.session = session
        self.states = {}
        self.pm.register(self)

    @st.hookimpl
    def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
        if not isinstance(value, BoundState):
            return
        self.states[key] = value.resource_state
        return value.data, value.resource_state.state.type

    def resolve(
        self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        return self.session.resolve(symbol, allow_unknowns, decode)

    def set_data(self, key: str, data: Any) -> None:
        return self.session.set_data(key, data)

    def delete_data(self, key: str) -> None:
        return self.session.delete_data(key)

    def dependency_graph(self) -> nx.MultiDiGraph:
        return self.session.dependency_graph()

    def resource_graph(self) -> "ResourceGraph":
        """
		Return a fully-rendered ResourceGraph for this session. Will fail with a resolution
		error if data is missing or there are unknowns or unresolved futures in the session.
		"""
        graph = ResourceGraph()
        dep_graph = self.dependency_graph()

        for key in self.ns.keys():
            ref = self.ns.ref(key)
            resolved = self.resolve(ref, decode=False)
            graph.set(
                key=key, value=resolved, type=ref.type, state=self.states.get(key)
            )

        for node in dep_graph:
            if dep_graph.pred[node]:
                graph.add_dependencies(node, list(dep_graph.pred[node]))

        return graph

    def resource_state(self, name: str) -> ResourceState:
        """
		Get the resource state for the given name, raising SymbolKeyError to indicate
		a failure
		"""
        if name not in self.states:
            raise exc.SymbolKeyError(name, self)
        return self.states[name]

    def get_bound_state(self, name: str) -> BoundState:
        state = self.resource_state(name)
        data = self.resolve(self.ns.ref(name))
        return BoundState(resource_state=state, data=data)

    def clone(self) -> "ResourceSession":
        cloned_session = self.session.clone()
        new_inst = type(self)(cloned_session)
        new_inst.states = self.states.copy()
        new_inst.pm = self.pm
        return new_inst


@dc.dataclass(frozen=True)
class ResourceGraph:
    """
	A resource graph is a wrapper around a simple graph that stores similar information
	to a resource session, but without symbols. Resource graphs are serializable. Note that
	not every node in a resource graph is necessarily a resource, it can contain any name
	just like a session.
	"""

    graph: nx.DiGraph = dc.field(default_factory=nx.DiGraph)

    def set(
        self,
        key: str,
        value: Any,
        type: types.Type,
        state: Optional[ResourceState] = None,
        remove_dependencies: bool = True,
    ) -> None:
        """
		Set the given value in the graph, with a state optionally specified for resources. Note
		that this operation will remove the current upstream edges of `key` unless
		remove_upstreams=False is specified as an argument
		"""
        if remove_dependencies and key in self.graph.nodes:
            for pred in list(self.graph.pred[key]):
                self.graph.remove_edge(pred, key)

        self.graph.add_node(key, value=value, type=type, state=state)

    def delete(self, key: str) -> None:
        """
		Delete the given key from this graph. Will raise KeyError if it doesn't exist
		"""
        if key not in self.graph.nodes:
            raise KeyError(key)
        self.graph.remove_node(key)

    def add_dependencies(self, key: str, dependencies: Sequence[str]) -> None:
        """
		Add dependencies to the given key
		"""
        if key not in self.graph.nodes:
            raise KeyError(key)
        for dep in dependencies:
            if dep not in self.graph.nodes:
                raise KeyError(dep)

        self.graph.add_edges_from([(dep, key) for dep in dependencies])

    async def refresh(self, registry: st.Registry) -> Iterator[str]:
        """
		Refresh the current state of all resources in the graph. Returns an asynchronous
		generator that yields keys as they finish refreshing successfully.
		"""

        async def handle_node(key):
            data = self.graph.nodes[key]
            if data["state"] is None:
                return
            state = data["state"]
            resource = registry.get_resource(state.resource_name)
            result = await resource.refresh(BoundState(state, data["value"]))
            self.set(
                key=key,
                value=result.data,
                type=type,
                state=result.resource_state,
                remove_dependencies=False,
            )

        node_list = list(self.graph.nodes)
        coros = list(map(handle_node, node_list))

        for key, coro in zip(node_list, asyncio.as_completed(coros)):
            await coro
            yield key

    def clone(self) -> "ResourceGraph":
        return type(self)(self.graph.copy())

    def to_dict(self, registry: "Registry") -> Dict[str, Any]:
        """
		Return a JSON-serializable dictionary representation of this resource graph.
		"""
        nodes = {}

        for node in self.graph.nodes:
            data = self.graph.nodes[node]

            if data["state"]:
                state_dict = data["state"].to_dict(registry)
                state_type = state_dict["state"].pop("type")
                nodes[node] = {
                    "data": data["value"],
                    "type": state_type,
                    "state": state_dict,
                    "depends_on": list(self.graph.pred[node]),
                }
            else:
                type_serializer = registry.get_type_serializer(data["type"])
                nodes[node] = {
                    "data": data["value"],
                    "type": type_serializer.serialize(data["type"]),
                    "state": None,
                    "depends_on": list(self.graph.pred[node]),
                }

        return nodes

    @classmethod
    def from_dict(cls, data: Any, registry: "Registry") -> "ResourceGraph":
        """
		Render a ResourceGraph from a JSON-serializable representation
		"""
        deps = {}
        instance = cls()

        for key, node in data.items():
            if node["state"] is not None:
                type_dict = node["type"]
                state_dict_copy = node["state"].copy()
                state_dict_copy["state"] = state_dict_copy["state"].copy()
                state_dict_copy["state"]["type"] = type_dict
                state = ResourceState.from_dict(state_dict_copy, registry)
                typ = state.state.type
                instance.set(key, node["data"], typ, state)
            else:
                type_serializer = registry.get_type_serializer_from_data(node["type"])
                typ = type_serializer.deserialize(node["type"])
                instance.set(key, node["data"], typ)

            deps[key] = node["depends_on"]

        for to_node, from_nodes in deps.items():
            instance.add_dependencies(to_node, from_nodes)

        return instance
