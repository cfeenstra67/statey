import abc
import dataclasses as dc
from typing import Any, Dict, Sequence, Iterable, Optional

import networkx as nx

from statey.syms import base


@dc.dataclass(frozen=True)
class Object(base.Proxy):
	"""
	A uniform API for structuring access to statey objects. Should not be
	called directly, use an API method to create objects
	"""
	impl: dc.InitVar[Any]
	type: dc.InitVar[Optional["Type"]] = None
	registry: dc.InitVar[Optional["Registry"]] = None
	__impl: "ObjectImplementation" = dc.field(init=False, default=None)
	__type: "Type" = dc.field(init=False, default=None)
	__registry: "Registry" = dc.field(init=False, default=None)
	__inst: "Proxy" = dc.field(init=False, default=None)

	def __post_init__(
		self,
		impl: Any,
		type: Optional["Type"],
		registry: Optional["Registry"]
	) -> None:
		"""
		Set up the Object instance
		"""
		import statey as st
		from statey.syms.impl import ObjectImplementation

		if isinstance(impl, Object):
			obj = impl
			impl = obj.__impl
			type = type or obj.__type
			registry = registry or obj.__registry

		elif not isinstance(impl, ObjectImplementation):
			if registry is None:
				registry = st.registry
			obj = registry.get_object(impl)
			impl = obj.__impl
			type = type or obj.__type

		if type is None:
			type = impl.type()

		if type is NotImplemented:
			raise ValueError(
				f"Object implementation {impl} does not have an implied type, one must "
				f"be passed manually."
			)

		if registry is None:
			registry = impl.registry()

		if registry is NotImplemented:
			registry = st.registry

		self.__dict__['__impl'] = impl
		self.__dict__['__type'] = type
		self.__dict__['__registry'] = registry

		impl_accessor = base.GetattrBasedAttributeAccess(self.__impl)
		self.__dict__['__inst'] = base.BoundAttributeAccess(self, impl_accessor)

	@property
	def __accessor(self) -> base.AttributeAccess:
		return base.OrderedAttributeAccess((
			self.__impl,
			self.__registry.get_methods(self.__type)
		))

	@property
	def i(self) -> base.Proxy:
		"""
		Convenient access to get an implementation-specific bound attribute accessor
		"""
		return base.BoundAttributeAccess(self, self.__impl)

	@property
	def m(self) -> base.Proxy:
		"""
		Convenient access for object methods
		"""
		return base.BoundAttributeAccess(self, self.__registry.get_methods(self.__type))

	@property
	def _(self) -> Any:
		"""
		If this object can be trivially resolved alone, do that here. Otherwise, just
		return self. This is useful to resolve something "as much as possible" without
		having to reason about whether something is an Object vs. a regular Python object
		"""
		res = self.__impl.apply_alone(self)
		return self if res is NotImplemented else res
