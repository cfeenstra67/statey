import abc
import dataclasses as dc
from collections import Counter
from functools import partial
from typing import Optional, Dict, Any, Sequence, Type as PyType, Callable, Tuple

import networkx as nx
import pluggy

import statey as st
from statey.task import TaskSession
from statey.syms import types, utils, session, plugins, symbols


@dc.dataclass(frozen=True)
class State:
	"""
	A state corresponds to some type for a resource.
	"""
	name: str
	type: types.Type
	null: bool = dc.field(repr=False, init=False, default=False)


@dc.dataclass(frozen=True)
class NullState(State):
	"""
	Null states must always have types.EmptyType as their type, so
	this is a helper to create such states.
	"""
	type: types.Type = dc.field(init=False, default=types.EmptyType)
	null: bool = dc.field(repr=False, init=False, default=True)


@dc.dataclass(frozen=True)
class ResourceState:
	"""
	A resource state is a state that is bound to some resource.
	"""
	state: State
	resource_name: str

	def __call__(self, arg=utils.MISSING, **kwargs) -> 'BoundState':
		"""
		Factory method for creating bound states for this resource state.
		"""
		if arg is utils.MISSING:
			arg = kwargs
		elif kwargs:
			raise ValueError('Either one positional arg or keyword arguments are required for a BoundState.')
		return BoundState(self, arg)


@dc.dataclass(frozen=True)
class BoundState:
	"""
	A bound state binds a resource state to some date.
	"""
	resource_state: ResourceState
	data: Any


class ResourceMeta(abc.ABCMeta):
	"""
	Metaclass for resources
	"""
	@classmethod
	def _validate_states(
		cls,
		old_states: Sequence[State],
		new_states: Sequence[State]
	) -> Tuple[Sequence[State], State]:

		new_names = Counter(state.name for state in new_states)
		if new_names and max(new_names.values()) > 1:
			multi = {k: v for k, v in new_names.items() if v > 1}
			raise ValueError(f'Duplicate states found: {multi}')

		old_states = [state for state in old_states if state.name not in new_names]
		return old_states + list(new_states)

	def __new__(cls, name: str, bases: Sequence[PyType], attrs: Dict[str, Any]) -> PyType:
		super_cls = super().__new__(cls, name, bases, attrs)
		states = super_cls.__states__ if hasattr(super_cls, '__states__') else ()
		new_states = [val for val in attrs.values() if isinstance(val, State)]
		states = cls._validate_states(states, new_states)
		super_cls.__states__ = tuple(states)
		return super_cls


class Resource(abc.ABC, metaclass=ResourceMeta):
	"""
	A resource represents a stateful object of some kind, and it can have one
	or more "states" that that object can exist in.
	"""
	def __init__(self) -> None:
		# This is temporary, should clean this up
		for state in self.__states__:
			self.set_resource_state(ResourceState(state, self.name))

	def set_resource_state(self, state: ResourceState) -> None:
		setattr(self, state.state.name, state)

	@property
	@abc.abstractmethod
	def name(self) -> str:
		raise NotImplementedError

	@property
	def null_state(self) -> ResourceState:
		state = next((s for s in self.__states__ if s.null))
		return ResourceState(state, self.name)

	@abc.abstractmethod
	def plan(
		self,
		current: BoundState,
		config: BoundState,
		session: TaskSession,
		input: symbols.Symbol
	) -> symbols.Symbol:
		"""
		Given a task session, the current state of a resource, and a task session with
		corresponding input reference, return an output reference that can be fully
		resolved when all the tasks in the task session have been complete successfully.
		"""
		raise NotImplementedError


class ResourceSession(session.Session):
	"""
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""
	def __init__(self, session: session.Session, unsafe: bool = False) -> None:
		super().__init__(session.ns, unsafe=unsafe)
		self.session = session
		self.states = {}
		self.pm.register(self)

	@st.hookimpl
	def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
		if not isinstance(value, BoundState):
			return
		self.states[key] = value.resource_state
		return value.data, value.resource_state.state.type

	def resolve(self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True) -> Any:
		return self.session.resolve(symbol, allow_unknowns, decode)

	def set_data(self, key: str, data: Any) -> None:
		return self.session.set_data(key, data)

	def dependency_graph(self) -> nx.MultiDiGraph:
		return self.session.dependency_graph()

	def resource_graph(self) -> nx.MultiDiGraph:
		graph = self.dependency_graph()
		utils.subgraph_retaining_dependencies(graph, list(self.states))
		for node in graph.nodes:
			graph.nodes[node]['state'] = self.states[node]
		return graph

	def clone(self) -> 'ResourceSession':
		cloned_session = self.session.clone()
		new_inst = type(self)(cloned_session)
		new_inst.states = self.states.copy()
		new_inst.pm = self.pm
		return new_inst
