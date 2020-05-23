import abc
import copy
import dataclasses as dc
from typing import Any, Optional, Callable, Dict

import statey as st
from statey.syms import types, utils, symbols


class Semantics(abc.ABC):
	"""
	Semantics define how we should treat a type within our symbolic API e.g.
	what type does an attribute of an instance of a certain type have?
	"""
	type: types.Type

	@abc.abstractmethod
	def attr_type(self, attr: Any) -> Optional[types.Type]:
		"""
		Return the type of the given attribute of this object, if any. A return
		value of None indicates that `attr` is not an attribute of this object.
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def get_attr(self, value: Any, attr: Any) -> Any:
		"""
		Given a value, get the attribute value from an encoded value
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def map(self, func: Callable[[Any], Any], value: Any) -> Any:
		"""
		Assuming that `value` is value or symbol of this type, return `value`
		applied to this object and any attributes (in that order)
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def clone(self, value: Any) -> Any:
		"""
		Return a semantically-correct deep copy of the given value
		"""
		raise NotImplementedError


@dc.dataclass(frozen=True)
class ValueSemantics(Semantics):
	"""
	Semantics for regular values--these are the default semantics but structural types offer
	additional functionality.
	"""
	type: types.Type

	def attr_type(self, attr: Any) -> Optional[types.Type]:
		return None

	def get_attr(self, value: Any, attr: Any) -> Any:
		raise AttributeError(attr)

	def map(self, func: Callable[[Any], Any], value: Any) -> Any:
		return func(value)

	@classmethod
	@st.hookimpl
	def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
		return cls(type)

	def clone(self, value: Any) -> Any:
		if isinstance(value, (symbols.Symbol, symbols.Unknown)):
			return value.clone()
		return copy.copy(value)


@dc.dataclass(frozen=True)
class ArraySemantics(Semantics):
	"""
	Semantics for ArrayTypes
	"""
	type: types.Type
	element_semantics: Semantics

	def attr_type(self, attr: Any) -> Optional[types.Type]:
		if isinstance(attr, int):
			return self.type.element_type
		if isinstance(attr, slice):
			return self.type
		el_type = self.element_semantics.attr_type(attr)
		if el_type is not None:
			return types.ArrayType(el_type, self.type.nullable)
		return None

	def get_attr(self, value: Any, attr: Any) -> Any:
		if value is None:
			return None
		if isinstance(value, symbols.Symbol):
			return value.get_attr(attr)
		if isinstance(attr, (int, slice)):
			return value[attr]
		# "exploding"
		out = []
		for item in value:
			out.append(self.element_semantics.get_attr(item, attr))
		return out

	def map(self, func: Callable[[Any], Any], value: Any) -> Any:
		value = func(value)
		if value is None:
			return None

		def process_value(x):
			return [
				self.element_semantics.map(func, item)
				for item in x
			]

		if isinstance(value, (symbols.Symbol, symbols.Unknown)):
			return value.map(process_value)
		return process_value(value)

	def clone(self, value: Any) -> Any:
		if value is None:
			return None

		if isinstance(value, (symbols.Symbol, symbols.Unknown)):
			return value.clone()
		return [self.element_semantics.clone(item) for item in value]

	@classmethod
	@st.hookimpl
	def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
		if not isinstance(type, types.ArrayType):
			return None
		element_semantics = registry.get_semantics(type.element_type)
		return cls(type, element_semantics)


@dc.dataclass(frozen=True)
class StructSemantics(Semantics):
	"""
	Semantics for StructTypes
	"""
	type: types.Type
	field_semantics: Dict[str, Semantics]

	def attr_type(self, attr: Any) -> Optional[types.Type]:
		if attr in self.type:
			return self.type[attr].type
		return None

	def get_attr(self, value: Any, attr: Any) -> Any:
		if value is None:
			return None
		if isinstance(value, symbols.Symbol):
			return value.get_attr(attr)
		return value[attr]

	def map(self, func: Callable[[Any], Any], value: Any) -> Any:
		value = func(value)
		if value is None:
			return None

		def process_value(x):
			return {
				key: self.field_semantics[key].map(func, val)
				for key, val in x.items()
			}

		if isinstance(value, (symbols.Symbol, symbols.Unknown)):
			return value.map(process_value)
		return process_value(value)

	def clone(self, value: Any) -> Any:
		if value is None:
			return None

		if isinstance(value, (symbols.Symbol, symbols.Unknown)):
			return value.clone()
		out = {}
		for key, val in value.items():
			out[key] = self.field_semantics[key].clone(val)
		return out

	@classmethod
	@st.hookimpl
	def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
		if not isinstance(type, types.StructType):
			return None
		field_semantics = {}
		for field in type.fields:
			field_semantics[field.name] = registry.get_semantics(field.type)
		return cls(type, field_semantics)


# Intentionally a list--this can be mutated if desired
SEMANTICS_CLASSES = [
	ValueSemantics,
	ArraySemantics,
	StructSemantics
]


def register() -> None:
	"""
	Replace default encoder with encoders defined here
	"""
	for cls in SEMANTICS_CLASSES:
		st.registry.pm.register(cls)
