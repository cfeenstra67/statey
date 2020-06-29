import abc
import dataclasses as dc
from typing import Any

from statey import hookimpl, Registry
from statey.syms import types, utils
from statey.syms.semantics import Semantics


class Interface(abc.ABC):
	"""
	An interface provides access to an object and its attributes and can
	implement additional methods for different types. This works together
	with semantics
	"""
	@property
	def type(self) -> types.Type:
		return self.semantics.type

	@property
	@abc.abstractmethod
	def semantics(self) -> Semantics:
		"""
		Get the type of the underlying object that this interface
		"""
		raise NotImplementedError

	@property
	@abc.abstractmethod
	def type(self) -> types.Type:
		"""
		Get the type of the underlying object that this interface
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def __getitem__(self, key: str) -> Interface:
		"""
		Get the interface for the given key
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def __getattr__(self, attr: str) -> Any:
		try:
			return getattr(super(), attr)
		except AttributeError as err:
			pass
		return self[attr]


@dc.dataclass(frozen=True)
class ObjectInterface:
	_data: Any
	_semantics

	@abc.abstractmethod
	def __getitem__(self, key: str) -> Interface:
		"""
		Get the interface for the given key
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def __getattr__(self, attr: str) -> Any:
		try:
			return getattr(super(), attr)
		except AttributeError as err:
			pass
		return self[attr]

	@hookimpl
    def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
    	if not isinstance(value, Interface):
    		return None
    	return value.get(), value.type


@dc.dataclass(frozen=True)
class DefaultInterface(Interface, utils.Cloneable):
	"""
	Default interface that just uses semantics to define get_attr behavior
	"""
	_data: Any
	_semantics: Semantics
	_registry: Registry

	@property
	def type(self) -> types.Type:
		return self._semantics.type

	@property
	def __getitem__(self, key: str) -> 'Interface':
		new_data = self._semantics.get_attr(self._data, key)
		new_semantics = self._semantics.attr_semantics(key)
		new_interface_factory = self._registry.get_interface_factory(new_semantics.type)
		return new_interface_factory(new_data)

	def get(self) -> Any:
		return self._data

	def _with(self, data: Any) -> Any:
		return self.clone(_data=data)

	@hookimpl
    def get_interface_factory(self, type: types.Type, registry: Registry) -> Callable[[Any], Interface]:
    	semantics = registry.get_semantics(type)
    	return lambda data: DefaultInterface(data, semantics, registry)


INTERFACE_CLASSES = [
	Interface,
	DefaultInterface
]


def register() -> None:
    """
    Register interface classes
	"""
    for cls in INTERFACE_CLASSES:
        st.registry.pm.register(cls)
