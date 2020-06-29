import abc
import dataclasses as dc
from typing import Any, Dict, Sequence

import statey as st
from statey.syms import func, impl, types, base
from statey.syms.object_ import Object
from statey.syms.semantics import Semantics


class Method(abc.ABC):
	"""
	A method is essentially a factory for creating "computed" attributes
	"""
	type: types.Type

	@abc.abstractmethod
	def bind(self, obj: Object, semantics: Semantics) -> Object:
		"""
		Given an input object, return some other object representing this attribute
		"""
		raise NotImplementedError


@dc.dataclass(frozen=True)
class InstanceMethod(Method):
	"""
	Instance method implementation
	"""
	func: func.Function

	@property
	def type(self) -> types.Type:
		return self.func.type

	def bind(self, obj: Object, semantics: Semantics) -> Object:
		new_impl = impl.FunctionCall(self.func, (obj,))
		return Object(new_impl, semantics=semantics)


class ObjectMethods(base.AttributeAccess):
	"""
	Accessor interface for object methods
	"""
	@abc.abstractmethod
	def get_method(self, name: str) -> Method:
		"""
		Get a method by name
		"""
		raise NotImplementedError

	def get_attr(self, obj: Object, attr: str) -> Object:
		method = self.get_method(attr)
		method_semantics = obj.__registry.get_semantics(method.type)

		return method.bind(obj, method_semantics)


@dc.dataclass(frozen=True)
class ConstantObjectMethods(ObjectMethods):
	"""
	ObjectMethods implementation from a dict of methods by name
	"""
	methods: Dict[str, Method]

	def get_method(self, name: str) -> Method:
		return self.methods[name]


NoMethods = ConstantObjectMethods({})


@dc.dataclass(frozen=True)
class CompositeObjectMethods(ObjectMethods):
	"""
	Class to combine multiple ObjectMethods instances into one
	"""
	object_methods: Sequence[ObjectMethods]

	def get_method(self, name: str) -> Method:
		for methods in self.object_methods:
			try:
				return methods.get_method(name)
			except KeyError:
				pass
		raise KeyError(name)


OBJECT_METHODS_CLASSES = [
	
]


def register() -> None:
    """
	Replace default object methods classes
	"""
    for cls in OBJECT_METHODS_CLASSES:
        st.registry.pm.register(cls)
