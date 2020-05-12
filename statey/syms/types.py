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



class Encoder(abc.ABC):
	"""
	An encoder encodes data of some with possibly native types info some format
	"""
	@abc.abstractmethod
	def encode(self, type: Type, value: Any) -> Any:
		"""
		Given a type and some _non-validated_ value, convert it to a serializable value
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def decode(self, type: Type, value: Any) -> Any:
		"""
		Given a freshly deserialized dictionary, potentially apply some post-processing or wrap
		it in a native type
		"""
		raise NotImplementedError


class EncoderHooks:
	"""
	Hooks to wrap encoder functionality
	"""
	@hookspec(firstresult=True)
	def encode(self, value: Any) -> Any:
		"""
		Optionally apply some logic to encode the given value. return None if the given value is not handled.
		"""

	@hookspec(firstresult=True)
	def decode(self, value: Any) -> Any:
		"""
		Opposite of the encode() hook
		"""


def create_encoder_plugin_manager():
	"""
	Factory function to create the default plugin manager for encoders
	"""
	pm = create_plugin_manager()
	pm.add_hookspecs(EncoderHooks)
	return pm


@dc.dataclass(frozen=True)
class DefaultEncoder(Encoder, utils.Cloneable):
	"""
	The default encoder just handles hooks properly, doesn't do any actual encoding
	"""
	pm: pluggy.PluginManager = dc.field(init=False, default_factory=create_encoder_plugin_manager, compare=False, repr=False)

	def encode(self, value: Any) -> Any:
		result = self.pm.hook.encode(value=value)
		return value if result is None else result

	def decode(self, value: Any) -> Any:
		result = self.pm.hook.decode(value=value)
		return value if result is None else result

	@hookimpl
	def get_encoder(self, type: Type) -> Encoder:
		"""
		The basic encoder behavior just calls hooks, but we should pass through plugins too.
		"""
		me_copy = self.clone()
		for plugin in self.pm.get_plugins():
			me_copy.pm.register(plugin)
		for plugin in type.pm.get_plugins():
			me_copy.pm.register(plugin)
		return me_copy


class TypeRegistry(abc.ABC):
	"""
	A type registry is used to parse annotations into types
	"""
	@abc.abstractmethod
	def get_type(self, annotation: Any, meta: Optional[Dict[str, Any]] = None) -> Type:
		"""
		Parse the given annotation and return a Type. This will properly handle dataclasses
		similarly to how case classes are handled in spark encoding
		"""
		raise NotImplementedError

	def infer_type(self, obj: Any) -> Type:
		"""
		Attempt to infer the type of `obj`, falling back on self.any_type
		"""
		annotation = utils.infer_annotation(obj)
		return self.get_type(annotation)

	@abc.abstractmethod
	def get_encoder(self, type: Type) -> Encoder:
		"""
		Given a type, return an Encoder instance to encode the type, raising an exc.NoEncoderFound to
		indicate failure
		"""
		raise NotImplementedError


class RegistryHooks:
	"""
	Specifies hooks for handling different annotations and converting them to types
	"""
	@hookspec(firstresult=True)
	def get_type(self, annotation: Any, registry: TypeRegistry, meta: Dict[str, Any]) -> Type:
		"""
		Handle the given annotation and return a Type, or None to indicate it can't be handled by this hook
		"""

	@hookspec(firstresult=True)
	def get_encoder(self, type: Type, registry: TypeRegistry) -> Encoder:
		"""
		Handle the given type and produce an Encoder instance that can encode values of that type
		"""


def create_registry_plugin_manager():
	"""
	Factory function to create the default base plugin manager for DefaultTypeRegistry
	"""
	pm = create_plugin_manager()
	pm.add_hookspecs(RegistryHooks)
	return pm


@dc.dataclass(frozen=True)
class DefaultTypeRegistry(TypeRegistry):

	any_type: Type = AnyType()
	pm: pluggy.PluginManager = dc.field(init=False, default_factory=create_registry_plugin_manager, compare=False, repr=False)

	def get_type(self, annotation: Any, meta: Optional[Dict[str, Any]] = None) -> Type:
		"""
		Parse the given annotation and return a Type. This will properly handle dataclasses
		similarly to how case classes are handled in spark encoding
		"""
		if meta is None:
			meta = {}
		handled = self.pm.hook.get_type(
			annotation=annotation,
			meta=meta,
			registry=self
		)
		return self.any_type if handled is None else handled

	def get_encoder(self, type: Type) -> Encoder:
		"""
		Given a type, get an Encoder instance that can encoder it
		"""
		handled = self.pm.hook.get_encoder(
			type=type,
			registry=self
		)
		if handled is None:
			raise exc.NoEncoderFound(type)
		return handled


def set_default_registry(new_registry: TypeRegistry) -> None:
	"""
	Configure the default registry to be the given value
	"""
	global registry
	registry = new_registry


registry = DefaultTypeRegistry()

default_encoder = DefaultEncoder()

registry.pm.register(default_encoder)
