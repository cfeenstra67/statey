import abc
import dataclasses as dc
import textwrap as tw
from functools import wraps
from typing import Sequence, Hashable, Type as PyType, Any, Dict, Callable, List, Optional, Tuple, Union

import marshmallow as ma
import pluggy

import statey as st
from statey.syms import types, utils, exc


class Encoder(abc.ABC):
	"""
	An encoder encodes data of some with possibly native types info some format
	"""
	@abc.abstractmethod
	def encode(self, type: types.Type, value: Any) -> Any:
		"""
		Given a type and some _non-validated_ value, convert it to a serializable value
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def decode(self, type: types.Type, value: Any) -> Any:
		"""
		Given a freshly deserialized dictionary, potentially apply some post-processing or wrap
		it in a native type
		"""
		raise NotImplementedError


class EncoderHooks:
	"""
	Hooks to wrap encoder functionality
	"""
	@st.hookspec(firstresult=True)
	def encode(self, value: Any) -> Any:
		"""
		Optionally apply some logic to encode the given value. return None if the given value is not handled.
		"""

	@st.hookspec(firstresult=True)
	def decode(self, value: Any) -> Any:
		"""
		Opposite of the encode() hook
		"""


def create_encoder_plugin_manager():
	"""
	Factory function to create the default plugin manager for encoders
	"""
	pm = st.create_plugin_manager()
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

	@st.hookimpl
	def get_encoder(self, type: types.Type) -> Encoder:
		"""
		The basic encoder behavior just calls hooks, but we should pass through plugins too.
		"""
		me_copy = self.clone()
		for plugin in self.pm.get_plugins():
			me_copy.pm.register(plugin)
		for plugin in type.pm.get_plugins():
			me_copy.pm.register(plugin)
		return me_copy


@dc.dataclass(frozen=True)
class MarshmallowEncoder(DefaultEncoder):
	"""
	Encodeable helper to get all functionality from a field factory
	"""
	type: types.Type
	pm: pluggy.PluginManager = dc.field(init=False, compare=False, repr=False, default_factory=create_encoder_plugin_manager)

	@abc.abstractmethod
	def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
		"""
		Return the marshmallow field for this type
		"""
		raise NotImplementedError

	def marshmallow_field(self, encoding: bool) -> ma.fields.Field:
		kws = self._marshmallow_field_kws(self.type.nullable)
		base = self.base_marshmallow_field(encoding)
		return utils.PossiblySymbolicField(base, self.type, **kws)

	def encode(self, data: Any) -> Any:
		# Allow pre-encoding hooks
		data = super().encode(data)
		field = self.marshmallow_field(True)
		with utils.reraise_ma_validation_error():
			# This does the validation
			data = field.deserialize(data)
		# This allows us to leverage marshmallow to do things like encoding
		# dates as strings w/ symmetrical encoding/decoding logic
		return field.serialize('tmp', {'tmp': data})

	def decode(self, data: Any) -> Any:
		with utils.reraise_ma_validation_error():
			value = self.marshmallow_field(False).deserialize(data)
			# Allow post-decoding hooks
			return super().decode(value)

	@staticmethod
	def _marshmallow_field_kws(nullable: bool) -> Dict[str, Any]:
		if nullable:
			return {'required': False, 'default': None, 'missing': None, 'allow_none': True}
		return {'required': True}


class MarshmallowValueEncoder(MarshmallowEncoder):
	"""
	Simple marshmallow encoder for value types
	"""
	base_field: ma.fields.Field
	type_cls: PyType[types.Type]

	def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
		return self.base_field

	@classmethod
	@st.hookimpl
	def get_encoder(cls, type: types.Type, registry: 'Registry') -> Encoder:
		if isinstance(type, cls.type_cls):
			instance = cls(type)
			for plugin in type.pm.get_plugins():
				instance.pm.register(plugin)
			return instance
		return None


@dc.dataclass(frozen=True)
class IntegerEncoder(MarshmallowValueEncoder):
	type_cls = types.IntegerType
	base_field = ma.fields.Int()


@dc.dataclass(frozen=True)
class FloatEncoder(MarshmallowValueEncoder):
	type_cls = types.FloatType
	base_field = ma.fields.Float()


@dc.dataclass(frozen=True, repr=False)
class BooleanEncoder(MarshmallowValueEncoder):
	type_cls = types.BooleanType
	base_field = ma.fields.Bool()


@dc.dataclass(frozen=True, repr=False)
class StringEncoder(MarshmallowValueEncoder):
	type_cls = types.StringType
	base_field = ma.fields.Str()


@dc.dataclass(frozen=True, repr=False)
class ArrayEncoder(MarshmallowEncoder):
	"""
	An array with some element type
	"""
	element_encoder: Encoder

	def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
		kws = self._marshmallow_field_kws(self.element_encoder.type.nullable)
		if encoding:
			kws['serialize'] = lambda x: x
			kws['deserialize'] = self.element_encoder.encode
		else:
			kws['serialize'] = lambda x: x
			kws['deserialize'] = self.element_encoder.decode

		element_field = utils.SingleValueFunction(**kws)
		return ma.fields.List(element_field)

	@classmethod
	@st.hookimpl
	def get_encoder(cls, type: types.Type, registry: 'Registry') -> Encoder:
		if not isinstance(type, types.ArrayType):
			return None
		element_encoder = registry.get_encoder(type.element_type)
		instance = cls(type, element_encoder)
		for plugin in type.pm.get_plugins():
			instance.pm.register(plugin)
		return instance


@dc.dataclass(frozen=True, repr=False)
class StructEncoder(MarshmallowEncoder):

	field_encoders: Dict[str, Encoder]

	def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
		return ma.fields.Nested(self.marshmallow_schema(encoding))

	def marshmallow_schema(self, encoding: bool) -> ma.Schema:
		fields = {}
		for name, encoder in self.field_encoders.items():
			kws = self._marshmallow_field_kws(encoder.type.nullable)
			if encoding:
				kws['serialize'] = lambda x: x
				kws['deserialize'] = encoder.encode
			else:
				kws['serialize'] = lambda x: x
				kws['deserialize'] = encoder.decode
			fields[name] = utils.SingleValueFunction(**kws)
		return type('StructSchema', (ma.Schema,), fields)()

	@classmethod
	@st.hookimpl
	def get_encoder(cls, type: types.Type, registry: 'Registry') -> Encoder:
		if not isinstance(type, types.StructType):
			return None
		encoders = {}
		for field in type.fields:
			encoders[field.name] = registry.get_encoder(field.type)
		instance = cls(type, encoders)
		for plugin in type.pm.get_plugins():
			instance.pm.register(plugin)
		return instance


# Intentionally a list--this can be mutated if desired
MARSHMALLOW_ENCODER_CLASSES = [
	IntegerEncoder,
	FloatEncoder,
	BooleanEncoder,
	StringEncoder,
	ArrayEncoder,
	StructEncoder
]


def register() -> None:
	"""
	Replace default encoder with encoders defined here
	"""
	for cls in MARSHMALLOW_ENCODER_CLASSES:
		st.registry.pm.register(cls)