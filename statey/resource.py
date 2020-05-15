import abc
import dataclasses as dc
from collections import Counter
from functools import partial
from typing import Optional, Dict, Any, Sequence, Type as PyType, Callable

import pluggy

import statey as st
from statey.syms import types, utils


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
	name: str
	resource_name: str
	data: Any
	# This can be passed as a manual type hint
	data_type: Optional[types.Type] = dc.field(
		repr=False, default=utils.MISSING, metadata={st.NS: {'encode': False}}
	)

	@classmethod
	@st.hookimpl
	def infer_type(cls, obj: Any, registry: 'Registry') -> types.Type:
		if not isinstance(obj, BoundState):
			return None
		if obj.data_type is utils.MISSING:
			return None
		from statey.syms.plugins import EncodeDataClassPlugin

		typ = types.StructType(
			[
				types.StructField('name', types.StringType(False)),
				types.StructField('resource_name', types.StringType(False)),
				types.StructField('data', obj.data_type)
			],
			False
		)
		typ.pm.register(EncodeDataClassPlugin(BoundState))
		return typ


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
				return BoundState(name=state.name, resource_name=self.name, data=x, data_type=state.type)
			self.set_state_factory(state, partial(state_factory, state))

	def set_state_factory(self, state: State, factory: Callable[[Any], BoundState]) -> None:
		setattr(self, state.name, factory)

	@property
	@abc.abstractmethod
	def name(self) -> None:
		raise NotImplementedError
