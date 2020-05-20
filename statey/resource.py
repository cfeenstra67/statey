import abc
import dataclasses as dc
from collections import Counter
from functools import partial
from typing import Optional, Dict, Any, Sequence, Type as PyType, Callable

import pluggy

import statey as st
from statey.syms import types, utils, session


@dc.dataclass(frozen=True)
class State:
	"""

	"""
	name: str
	type: Optional[types.Type]


@dc.dataclass(frozen=True)
class BoundState:
	"""

	"""
	resource_name: str
	state: State
	data: Any

	@st.hookimpl
	@classmethod
	def infer_type(cls, obj: Any, registry: st.Registry) -> types.Type:
		if not isinstance(obj, BoundState):
			return None

			










class ResourceMeta(abc.ABCMeta):
	"""

	"""
	@classmethod
	def _validate_states(cls, old_states: Sequence[State], new_states: Sequence[State]) -> Sequence[State]:
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

	"""
	def __init__(self) -> None:
		# This is temporary, should clean this up
		for state in self.__states__:
			def state_factory(state, x=utils.MISSING, **kwargs):
				if x is utils.MISSING:
					x = kwargs
				elif kwargs:
					raise ValueError('Either one positional arg or keyword arguments are required for a BoundState.')
				return BoundState(state=state, resource_name=self.name, data=x)
			self.set_state_factory(state, partial(state_factory, state))

	def set_state_factory(self, state: State, factory: Callable[[Any], BoundState]) -> None:
		setattr(self, state.name, factory)

	@property
	@abc.abstractmethod
	def name(self) -> None:
		raise NotImplementedError


class ResourceSession(session.Session):
	"""
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""
	def __init__(self, session: session.Session) -> None:
		self.session = session
		self.states = {}

	def resolve(self, symbol: symbols.Symbol) -> Any:
		return self.session.resolve(symbol)

	def set_data(self, key: str, data: Any) -> None:
		if isinstance(data)

		self.session.set_data(key, data)





