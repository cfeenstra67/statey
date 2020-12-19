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
from statey.helpers import async_providers_context
from statey.provider import ProviderId, Provider, ProviderIdSchema
from statey.task import TaskSession
from statey.syms import types, utils, session, impl, Object, py_session


class StateSchema(ma.Schema):
    """
    Marshmallow schema for extra safety encoding/decoding states
    """

    name = ma.fields.Str(required=True, default=None)
    input_type = ma.fields.Dict(required=True, default=None)
    output_type = ma.fields.Dict(required=True, default=None)
    null = ma.fields.Bool(required=True, default=None)


class AbstractState(abc.ABC):
    """
    Abstract base class defining required attributes for states
    """

    name: str
    input_type: types.Type
    output_type: types.Type
    null: bool

    @property
    def type(self) -> types.Type:
        return self.output_type

    def to_dict(self, registry: "Registry") -> Dict[str, Any]:
        """
        Render this state to a JSON-serializable dictionary
        """
        input_type_serializer = registry.get_type_serializer(self.input_type)
        output_type_serializer = registry.get_type_serializer(self.output_type)
        out = {
            "name": self.name,
            "input_type": input_type_serializer.serialize(self.input_type),
            "output_type": output_type_serializer.serialize(self.output_type),
            "null": self.null,
        }
        return StateSchema().dump(out)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], registry: "Registry") -> "State":
        """
        Render a State from the output of to_dict()
        """
        data = StateSchema().load(data)
        input_type_serializer = registry.get_type_serializer_from_data(
            data["input_type"]
        )
        input_type = input_type_serializer.deserialize(data["input_type"])
        output_type_serializer = registry.get_type_serializer_from_data(
            data["output_type"]
        )
        output_type = output_type_serializer.deserialize(data["output_type"])
        return cls(
            name=data["name"],
            input_type=input_type,
            output_type=output_type,
            null=data["null"],
        )


@dc.dataclass(frozen=True)
class State(AbstractState):
    """
    A state corresponds to some type for a resource.
    """

    name: str
    input_type: types.Type
    output_type: types.Type
    null: bool = dc.field(repr=False, default=False)


@dc.dataclass(frozen=True)
class NullState(State):
    """
    Null states must always have types.EmptyType as their type, so
    this is a helper to create such states.
    """

    input_type: types.Type = dc.field(init=False, default=types.EmptyType)
    output_type: types.Type = dc.field(init=False, default=types.EmptyType)
    null: bool = dc.field(repr=False, init=False, default=True)


class ResourceStateSchema(ma.Schema):
    """
    Marshmallow schema for extra safety encoding/decoding resource states
    """

    state = ma.fields.Nested(StateSchema(), required=True, default=None)
    resource = ma.fields.Str(required=True, default=None)
    provider = ma.fields.Nested(ProviderIdSchema(), required=True, default=None)


@dc.dataclass(frozen=True)
class ResourceState:
    """
    A resource state is a state that is bound to some resource.
    """

    state: AbstractState
    resource: str
    provider: ProviderId

    @property
    def name(self) -> str:
        return self.state.name

    @property
    def input_type(self) -> types.Type:
        return self.state.input_type

    @property
    def output_type(self) -> types.Type:
        return self.state.output_type

    @property
    def type(self) -> types.Type:
        return self.state.type

    @property
    def null(self) -> bool:
        return self.state.null

    def snapshot(self, data: Any) -> "StateSnapshot":
        """
        Create a StateSnapshot from this resource state and the given data
        """
        return StateSnapshot(data, self)

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
            "resource": self.resource,
            "state": self.state.to_dict(registry),
            "provider": self.provider.to_dict(),
        }
        return ResourceStateSchema().dump(out)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], registry: "Registry") -> "ResourceState":
        """
        Render a ResourceState from the output of to_dict()
        """
        data = ResourceStateSchema().load(data)
        state = State.from_dict(data["state"], registry)
        provider_id = ProviderId.from_dict(data["provider"])
        return cls(resource=data["resource"], state=state, provider=provider_id)


@dc.dataclass(frozen=True)
class BoundState(utils.Cloneable):
    """
    Describes an input configuration for a resource
    """

    state: ResourceState
    input: "Object"
    output: "Object" = dc.field(init=False, default=None)

    def __post_init__(self) -> None:
        input_obj = st.Object(self.input, self.state.input_type)
        self.__dict__["input"] = input_obj
        output_impl = impl.Unknown(
            refs=(input_obj,), return_type=self.state.output_type
        )
        self.__dict__["output"] = st.Object(output_impl)

    def bind(self, registry: "Registry") -> None:
        """
        Bind input and output objects to the given registry
        """
        self.__dict__["input"] = st.Object(self.input, registry=registry)
        self.__dict__["output"] = st.Object(self.output, registry=registry)


class StateTuple(abc.ABC, utils.Cloneable):
    """
    Binding a state to a value
    """

    obj: "Object"
    state: ResourceState

    @property
    @abc.abstractmethod
    def type(self) -> types.Type:
        """
        Return the type of `data`
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_obj(self, registry: Optional["Registry"] = None) -> "Object":
        """
        Obtain an object representation of this StateTuple
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class StateConfig(StateTuple):
    """
    A state configuration object, may contain unknowns
    """

    obj: "Object"
    state: ResourceState

    @property
    def type(self) -> types.Type:
        return self.state.input_type

    def get_obj(self, registry: Optional["Registry"] = None) -> "Object":
        if registry is None:
            registry = st.registry
        return st.Object(self.obj._impl, self.obj._type, registry)


@dc.dataclass(frozen=True)
class StateSnapshot(StateTuple):
    """
    A fully-resolved snapshot of a resource state
    """

    data: Any
    state: ResourceState

    @property
    def type(self) -> types.Type:
        return self.state.output_type

    def get_obj(self, registry: Optional["Registry"] = None) -> "Object":
        if registry is None:
            registry = st.registry
        return st.Object(self.data, self.type, registry)

    @property
    def obj(self) -> "Object":
        return self.get_obj()


class Resource(abc.ABC):
    """
    A resource represents a stateful object of some kind, and it can have one
    or more "states" that that object can exist in.
    """

    name: str
    provider: Provider

    @property
    @abc.abstractmethod
    def null_state(self) -> ResourceState:
        """
        Return the null state of this resource, at which point it will removed from
        resource graphs
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def plan(
        self, current: StateSnapshot, config: StateConfig, session: TaskSession
    ) -> Object:
        """
        Given a task session, the current state of a resource, and a task session with
        corresponding input reference, return an output reference that can be fully
        resolved when all the tasks in the task session have been complete successfully.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def refresh(self, current: StateSnapshot) -> StateSnapshot:
        """
        Given the current bound state, return a new bound state that represents that actual
        current state of the object
        """
        raise NotImplementedError

    async def finalize(self, data: StateSnapshot) -> StateSnapshot:
        """
        Called on states before they are committed to the resource graph. By default a noop
        """
        return data


class ResourceSession(session.WriteableSession):
    """
    Session subclass that wraps a regular session but handles resources in a special manner.
    """

    def __init__(
        self, session: session.Session, states: Optional[Dict[str, BoundState]] = None
    ) -> None:
        if states is None:
            states = {}

        super().__init__(session.ns)
        self.session = session
        self.states = states
        self.pm.register(self)

    @st.hookimpl
    def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
        if not isinstance(value, BoundState):
            return
        # Bind the config to this registry
        value.bind(self.ns.registry)
        self.states[key] = value
        return value.output, value.output._type

    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        return self.session.resolve(symbol, allow_unknowns, decode)

    def set_data(self, key: str, data: Any) -> None:
        return self.session.set_data(key, data)

    def get_data(self, key: str) -> Any:
        return self.session.get_data(key)

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
            if key in self.states:
                state = self.states[key]
                resolved = self.resolve(self.ns.ref(key), decode=False)
                graph.set(
                    key=key,
                    value=resolved,
                    type=state.state.output_type,
                    state=state.state,
                )
            else:
                resolved = self.resolve(self.ns.ref(key), decode=False)
                graph.set(key=key, value=resolved, type=self.ns.resolve(key))

        for node in dep_graph:
            if dep_graph.pred[node]:
                graph.add_dependencies(node, list(dep_graph.pred[node]))

        return graph

    def get_state(self, name: str) -> BoundState:
        """
        Get the resource state for the given name, raising SymbolKeyError to indicate
        a failure
        """
        if name not in self.states:
            raise exc.SymbolKeyError(name, self)
        return self.states[name]

    def clone(self) -> "ResourceSession":
        new_inst = type(self)(self.session.clone())
        new_inst.states = self.states.copy()
        for plugin in self.pm.get_plugins():
            if plugin is not self:
                new_inst.pm.register(plugin)
        return new_inst


def create_resource_session(
    session: Optional[session.Session] = None,
) -> ResourceSession:
    """
    Convenient factory function for creating resource sessions.
    """
    if session is None:
        import statey as st

        session = st.create_session()
    return ResourceSession(session)


@dc.dataclass(frozen=True)
class ResourceGraph:
    """
    A resource graph is a wrapper around a simple graph that stores similar information
    to a resource session, but without objects. Resource graphs are serializable. Note that
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

    def get(self, key: str) -> Dict[str, Any]:
        """
        Get the data about this key stored in the resource graph
        """
        return self.graph.nodes[key]

    def keys(self) -> Sequence[str]:
        """
        Get a list of keys currently stored in this graph
        """
        return list(self.graph.nodes)

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

    async def refresh(
        self, registry: Optional[st.Registry] = None, finalize: bool = False
    ) -> Iterator[str]:
        """
        Refresh the current state of all resources in the graph. Returns an asynchronous
        generator that yields keys as they finish refreshing successfully.
        """
        if registry is None:
            registry = st.registry

        providers = {}

        for node in self.graph.nodes:
            state = self.graph.nodes[node]["state"]
            if state is None:
                continue
            provider = providers.get(state.provider)
            if provider is None:
                provider = providers[state.provider] = registry.get_provider(
                    *state.provider
                )

        async def handle_node(key):
            data = self.graph.nodes[key]
            state = data["state"]
            if state is None:
                return
            provider = providers[state.provider]
            resource = provider.get_resource(state.resource)
            result = await resource.refresh(StateSnapshot(data["value"], state))
            if finalize:
                result = await resource.finalize(result)
            self.set(
                key=key,
                value=result.data,
                type=result.type,
                state=result.state,
                remove_dependencies=False,
            )

        providers_list = list(providers.values())
        node_list = list(self.graph.nodes)
        coros = list(map(handle_node, node_list))

        async with async_providers_context(providers_list):
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

            type_serializer = registry.get_type_serializer(data["type"])
            state_dict = None
            if data["state"]:
                state_dict = data["state"].to_dict(registry)
                del state_dict["state"]["output_type"]

            nodes[node] = {
                "data": data["value"],
                "type": type_serializer.serialize(data["type"]),
                "state": state_dict,
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
                state_dict_copy["state"]["output_type"] = type_dict
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

    def to_session(self) -> ResourceSession:
        """
        Construct a session with the same data and dependencies as this ResourceGraph
        """
        core_session = ResourceGraphSession(py_session.PythonNamespace(), graph=self)
        states = {}

        for key in self.keys():
            data = self.get(key)
            core_session.ns.new(key, data["type"])
            state = data["state"]
            if state is not None:
                states[key] = BoundState(state, st.Unknown[state.input_type])

        return ResourceSession(core_session, states)


class ResourceGraphSession(py_session.PythonSession):
    """"""

    def __init__(self, *args, graph: ResourceGraph, **kwargs) -> None:
        self.graph = graph
        super().__init__(*args, **kwargs)

    def get_encoded_root(self, key: str) -> Any:
        return self.graph.get(key)["value"]

    def dependency_graph(self) -> nx.MultiDiGraph:
        out = nx.MultiDiGraph()

        for key in self.graph.keys():
            out.add_node(key)

        out.add_edges_from(self.graph.graph.edges)

        return out

    def clone(self) -> "ResourceGraphSession":
        return type(self)(self.ns.clone(), graph=self.graph)
