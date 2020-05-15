import abc
import dataclasses as dc
import textwrap as tw
from functools import lru_cache
from typing import Sequence, Hashable, Type as PyType, Any, Dict, Callable, List, Optional

import pluggy

from statey import hookimpl, hookspec, create_plugin_manager
from statey.syms import utils, exc


def create_type_plugin_manager():
	"""
	Factory function for plugin managers for types, registering any default hook specs
	"""
	from .encoders import EncoderHooks

	pm = create_plugin_manager()
	pm.add_hookspecs(EncoderHooks)
	return pm


class Type(abc.ABC):
	"""
	A type encapsulates information about 
	"""
	name: str
	nullable: bool
	pm: pluggy.PluginManager

	def __repr__(self) -> str:
		return f'{self.name}{"?" if self.nullable else ""}'


class ValueType(Type):
	"""
	Base class for types that just have a single instance
	"""
	nullable: bool
	pm: pluggy.PluginManager

	@property
	@abc.abstractmethod
	def name(self) -> str:
		raise NotImplementedError


@dc.dataclass(frozen=True, repr=False)
class AnyType(Type):
	"""
	Type that applies to any value. Not a regular dataclass to avoid issues w/ defaults :(
	"""
	pm: pluggy.PluginManager = dc.field(init=False, default_factory=create_type_plugin_manager, compare=False)

	@property
	def nullable(self) -> bool:
		return True

	@property
	def name(self) -> str:
		return 'any'


class NumberType(ValueType, AnyType):
	"""
	Base class for numeric types
	"""
	@abc.abstractmethod
	def value_from_number(self, original: Any) -> Any:
		raise NotImplementedError


@dc.dataclass(frozen=True, repr=False)
class IntegerType(NumberType):
	nullable: bool = True
	name: str = dc.field(init=False, repr=False, default='integer')

	def value_from_number(self, original: Any) -> Any:
		return int(original)


@dc.dataclass(frozen=True, repr=False)
class FloatType(NumberType):
	nullable: bool = True
	name: str = dc.field(init=False, repr=False, default='float')

	def value_from_number(self, original: Any) -> Any:
		return float(original)


@dc.dataclass(frozen=True, repr=False)
class BooleanType(NumberType):
	nullable: bool = True
	name: str = dc.field(init=False, repr=False, default='boolean')

	def value_from_number(self, original: Any) -> Any:
		return bool(original)


@dc.dataclass(frozen=True, repr=False)
class StringType(ValueType, AnyType):
	nullable: bool = True
	name: str = dc.field(init=False, repr=False, default='string')


@dc.dataclass(frozen=True, repr=False)
class ArrayType(AnyType):
	"""
	An array with some element type
	"""
	element_type: Type
	nullable: bool = True

	@property
	def name(self) -> str:
		return 'array'
	
	def __repr__(self) -> str:
		return f'{self.name}[{repr(self.element_type)}]{"?" if self.nullable else ""}'


@dc.dataclass(frozen=True)
class StructField:
	"""
	Contains information about a single field in a StructType
	"""
	name: str
	type: Type


@dc.dataclass(frozen=True)
class StructType(AnyType):
	"""
	A struct contains an ordered sequence of named fields, any of which
	may or may not be null
	"""
	fields: Sequence[StructField]
	nullable: bool = True

	@property
	def name(self) -> str:
		return 'struct'

	def __repr__(self) -> str:
		field_strings = [
			f'{field.name}:{repr(field.type)}'
			for field in self.fields
		]
		return f'{self.name}[{", ".join(field_strings)}]{"?" if self.nullable else ""}'

	def __getitem__(self, name: str) -> StructField:
		"""
		Fetch schema fields by name
		"""
		field = next((field for field in self.fields if field.name == name), None)
		if field is None:
			raise KeyError(name)
		return field

	def __contains__(self, name: str) -> bool:
		"""
		Indicate whether this StructType has a field with the given name
		"""
		return any(field.name == name for field in self.fields)
