"""
Base classes for basic functionality used in several different types of objects
"""
import abc
import dataclasses as dc
from typing import Any, Sequence


class AttributeAccess(abc.ABC):
	"""
	Defines some interface for getting attributes for some object
	"""
	@abc.abstractmethod
	def get_attr(self, obj: Any, attr: str) -> Any:
		"""
		Get some attribute by name on the given object
		"""
		raise NotImplementedError


@dc.dataclass(frozen=True)
class OrderedAttributeAccess(AttributeAccess):
	"""
	Combine a set of AttributeAccess instances and attempt to use them in order
	"""
	accessors: Sequence[AttributeAccess]

	def get_attr(self, obj: Any, attr: str) -> Any:
		for accessor in self.accessors:
			try:
				return accessor.get_attr(self, attr)
			except KeyError:
				pass
		raise KeyError(attr)


@dc.dataclass(frozen=True)
class GetattrBasedAttributeAccess(AttributeAccess):
	"""
	Simple default python attribute access
	"""
	obj: Any

	@property
	def __instance(self) -> Any:
		return self.obj

	def get_attr(self, obj: Any, attr: str) -> Any:
		func = getattr(self.obj, attr)
		return func(obj, attr)


class Proxy(abc.ABC):
	"""
	Defines a generic way to bind an attribute access object to some instance
	"""
	@property
	def __instance(self) -> Any:
		"""
		Allow use of this class 
		"""
		return self

	@property
	@abc.abstractmethod
	def __accessor(self) -> AttributeAccess:
		"""
		Get the AttributeAccess instance for this instance
		"""
		raise NotImplementedError

	def __getitem__(self, attr: Any) -> Any:
		"""
		Main API method for accessing attributes. Test all accessors in order.
		"""
		return self.__accessor.get_attr(self.__instance, attr)

	def __getattr__(self, attr: Any) -> Any:
		try:
			return getattr(super(), attr)
		except AttributeError:
			pass
		return self[attr]


@dc.dataclass(frozen=True)
class BoundAttributeAccess(Proxy):
	"""
	Simple object to allow attribute access on an arbitrary instance
	"""
	instance: dc.InitVar[Any]
	accessor: dc.InitVar[AttributeAccess]
	__instance: Any = dc.field(init=False, default=None)
	__accessor: AttributeAccess = dc.field(init=False, default=None)

	def __post_init__(self, obj: Any, accessor: AttributeAccess) -> None:
		self.__dict__['__instance'] = obj
		self.__dict__['__accessor'] = accessor
