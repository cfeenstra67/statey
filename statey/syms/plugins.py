import dataclasses as dc
from typing import Tuple, Type, Dict, Any, Type as PyType, Union, Callable, Sequence

import statey as st
from statey.syms import types, utils


# Default Plugin definitions 
@dc.dataclass(frozen=True)
class HandleOptionalPlugin:
	"""
	Handle an Optional[] annotation wrapper
	"""
	@st.hookimpl
	def get_type(self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]) -> types.Type:
		inner = utils.extract_optional_annotation(annotation)
		if inner is None:
			return None
		meta['nullable'] = True
		return registry.get_type(inner, meta)


@dc.dataclass(frozen=True)
class ValuePredicatePlugin:
	"""
	Simple predicate plugin that will may an annotation to a ValueType subclass (or any
	whos constructor is just the nullable argument)
	"""
	predicate: Union[Callable[[Any], bool], PyType]
	type_cls: PyType[types.ValueType]

	@st.hookimpl(tryfirst=True)
	def get_type(self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]) -> types.Type:
		predicate = self.predicate
		if isinstance(self.predicate, type):
			predicate = lambda x: isinstance(x, type) and issubclass(x, self.predicate)

		if not predicate(annotation):
			return None

		return self.type_cls(meta.get('nullable', False))


@dc.dataclass(frozen=True)
class ParseSequencePlugin:
	"""
	Parse lists and sequences into ArrayTypes
	"""
	array_type_cls: PyType[types.ArrayType] = types.ArrayType

	@st.hookimpl
	def get_type(self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]) -> types.Type:
		if not isinstance(annotation, type) or not issubclass(annotation, Sequence):
			return None
		inner = utils.extract_inner_annotation(annotation)
		# Optionals are subtypes of themselves I guess?
		if utils.extract_optional_annotation(annotation) is not None:
			return None
		element_type = registry.get_type(inner) if inner else registry.any_type
		return self.array_type_cls(element_type, meta.get('nullable', False))


@dc.dataclass(frozen=True)
class ParseDataClassPlugin:
	"""
	Parse a specific dataclass into a StructType
	"""
	dataclass_cls: PyType
	struct_type_cls: PyType[types.StructType] = types.StructType

	@st.hookimpl
	def get_type(self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]) -> types.Type:
		if annotation is not self.dataclass_cls or not dc.is_dataclass(annotation):
			return None
		fields = []
		for dc_field in utils.encodeable_dataclass_fields(annotation):
			field_annotation = dc_field.type
			syms_type = registry.get_type(field_annotation)
			syms_field = types.StructField(dc_field.name, syms_type)
			fields.append(syms_field)
		instance = self.struct_type_cls(tuple(fields), meta.get('nullable', False))
		# Register encoding hooks
		instance.pm.register(EncodeDataClassPlugin(self.dataclass_cls, self.struct_type_cls))
		return instance


@dc.dataclass(frozen=True)
class EncodeDataClassPlugin:
	"""
	Parse a specific dataclass into a StructType
	"""
	dataclass_cls: PyType
	struct_type_cls: PyType[types.StructType] = types.StructType

	@st.hookimpl
	def decode(self, value: Any) -> Any:
		return self.dataclass_cls(**value) if value is not None else None

	@st.hookimpl
	def encode(self, value: Any) -> Any:
		if not isinstance(value, self.dataclass_cls) or not dc.is_dataclass(value):
			return None
		return {field.name: getattr(value, field.name) for field in utils.encodeable_dataclass_fields(value)}


def default_plugins() -> Sequence[Any]:
	"""
	Generate the default predicates dictionary, optionally replacing any of the default
	classes
	"""
	return [
		HandleOptionalPlugin(),
		ValuePredicatePlugin(float, types.FloatType),
		ValuePredicatePlugin(int, types.IntegerType),
		ValuePredicatePlugin(list, types.ArrayType),
		ValuePredicatePlugin(str, types.StringType),
		ValuePredicatePlugin(bool, types.BooleanType),
		ParseSequencePlugin(types.ArrayType),
	]


def register() -> None:
	"""
	Register default plugins
	"""
	for plugin in default_plugins():
		st.registry.pm.register(plugin)
