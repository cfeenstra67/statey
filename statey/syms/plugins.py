import collections
import dataclasses as dc
import operator
from functools import reduce, partial
from typing import Type as PyType, Dict, Any, Union, Callable, Sequence, Optional

import statey as st
from statey.syms import types, utils, impl


# Default Plugin definitions
@dc.dataclass(frozen=True)
class HandleOptionalPlugin:
    """
    Handle an Optional[] annotation wrapper
    """

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        inner = utils.extract_optional_annotation(annotation)
        if inner is None:
            return None
        meta["nullable"] = True
        return registry.get_type(inner, meta)


@dc.dataclass(frozen=True)
class ValuePredicatePlugin:
    """
    Simple predicate plugin that will may an annotation to any subclass
    whos constructor is just the nullable and meta arguments
    """

    predicate: Union[Callable[[Any], bool], PyType]
    type_cls: PyType[types.Type]

    @st.hookimpl(tryfirst=True)
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        predicate = self.predicate
        if isinstance(self.predicate, type):
            predicate = lambda x: isinstance(x, type) and issubclass(x, self.predicate)

        if not predicate(annotation):
            return None

        meta = meta.copy()
        return self.type_cls(meta.pop("nullable", False), meta=meta)


@dc.dataclass(frozen=True)
class AnyPlugin:
    """
    Plugin that will always return AnyType. Should be added FIRST
    """

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        return types.AnyType(meta=meta)


@dc.dataclass(frozen=True)
class ParseSequencePlugin:
    """
    Parse lists and sequences into ArrayTypes
    """

    array_type_cls: PyType[types.ArrayType] = types.ArrayType

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        # Sort of hacky, will behave differently in different python versions (3.7 works)
        if not utils.is_sequence_annotation(annotation):
            return None

        inner = utils.extract_inner_annotation(annotation)
        # Optionals are subtypes of themselves I guess?
        if utils.extract_optional_annotation(annotation) is not None:
            return None
        element_type = registry.get_type(inner) if inner else st.Any
        meta = meta.copy()
        return self.array_type_cls(element_type, meta.pop("nullable", False), meta=meta)

    @st.hookimpl
    def infer_type(self, obj: Any, registry: "Registry") -> types.Type:
        if not isinstance(obj, (list, tuple)):
            return None

        element_types = []
        for item in obj:
            typ = registry.infer_type(item)
            if typ not in element_types:
                element_types.append(typ)

        if not element_types:
            return None
        if len(element_types) > 1:
            return None
        return types.ArrayType(element_types.pop(), False)


@dc.dataclass(frozen=True)
class ParseMappingPlugin:
    """
    Parse lists and sequences into ArrayTypes
    """

    map_type_cls: PyType[types.MapType] = types.MapType

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        # Sort of hacky, will behave differently in different python versions (3.7 works)
        if not utils.is_mapping_annotation(annotation):
            return None

        inners = utils.extract_inner_annotations(annotation)
        # Optionals are subtypes of themselves I guess?
        if utils.extract_optional_annotation(annotation) is not None:
            return None
        if inners is not None and len(inners) != 2:
            return None
        key_annotation, value_annotation = inners or (None, None)
        key_type = registry.get_type(key_annotation) if key_annotation else st.Any
        value_type = registry.get_type(value_annotation) if value_annotation else st.Any
        meta = meta.copy()
        return self.map_type_cls(
            key_type, value_type, meta.pop("nullable", False), meta=meta
        )


@dc.dataclass(frozen=True)
class ParseDataClassPlugin:
    """
    Parse a specific dataclass into a StructType
    """

    dataclass_cls: PyType
    struct_type_cls: PyType[types.StructType] = types.StructType

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        if annotation is not self.dataclass_cls or not dc.is_dataclass(annotation):
            return None
        fields = []
        for dc_field in utils.encodeable_dataclass_fields(annotation):
            field_annotation = dc_field.type
            syms_type = registry.get_type(field_annotation)
            syms_field = types.StructField(dc_field.name, syms_type)
            fields.append(syms_field)
        meta = meta.copy()
        meta.update(
            {
                "plugins": [
                    EncodeDataClassPlugin(self.dataclass_cls, self.struct_type_cls)
                ]
            }
        )
        instance = self.struct_type_cls(
            tuple(fields), meta.pop("nullable", False), meta=meta
        )
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
        return {
            field.name: getattr(value, field.name)
            for field in utils.encodeable_dataclass_fields(value)
        }


@dc.dataclass(frozen=True)
class LiteralPlugin:
    """
    Create Data literals from python objects whose types can be inferred directly
    """

    @st.hookimpl
    def get_object(self, value: Any, registry: st.Registry) -> "Object":
        if isinstance(value, st.Object):
            return None
        try:
            value_type = registry.infer_type(value)
        except st.exc.NoTypeFound:
            return None
        return st.Object(impl.Data(value, value_type), registry=registry)


@dc.dataclass(frozen=True)
class StructFromDictPlugin:
    """
    Attempts to construct a struct type given a dictionary input
    """

    @st.hookimpl
    def infer_type(self, obj: Any, registry: "Registry") -> types.Type:
        if not isinstance(obj, dict) or not all(isinstance(key, str) for key in obj):
            return None

        fields = []
        for name, value in obj.items():
            try:
                field_type = registry.infer_type(value)
            except st.exc.NoTypeFound:
                return None
            fields.append(st.Field(name, field_type))

        return types.StructType(fields, False)


@dc.dataclass(frozen=True)
class BasicObjectBehaviors:
    """
    Basic behavior for inferring types from Objects
    """

    @st.hookimpl
    def get_object(self, value: Any, registry: st.Registry) -> "Object":
        if isinstance(value, st.Object):
            return value
        return None

    @st.hookimpl
    def infer_type(self, obj: Any, registry: "Registry") -> types.Type:
        if isinstance(obj, st.Object):
            return obj._type
        return None

    @st.hookimpl
    def get_type(
        self, annotation: Any, registry: st.Registry, meta: Dict[str, Any]
    ) -> types.Type:
        if isinstance(annotation, types.Type):
            meta = meta.copy()
            type_as_nullable = annotation.with_nullable(
                meta.pop("nullable", annotation.nullable)
            )
            type_meta = type_as_nullable.meta.copy()
            type_meta.update(meta)
            return type_as_nullable.with_meta(type_meta)
        return None


DEFAULT_PLUGINS = [
    AnyPlugin(),
    HandleOptionalPlugin(),
    ParseSequencePlugin(types.ArrayType),
    ParseMappingPlugin(types.MapType),
    ValuePredicatePlugin(float, types.FloatType),
    ValuePredicatePlugin(int, types.IntegerType),
    ValuePredicatePlugin(list, partial(types.ArrayType, types.Any)),
    ValuePredicatePlugin(str, types.StringType),
    ValuePredicatePlugin(bool, types.BooleanType),
    ValuePredicatePlugin(range, partial(types.ArrayType, types.Integer)),
    ValuePredicatePlugin((lambda x: x is Any), types.AnyType),
    LiteralPlugin(),
    BasicObjectBehaviors(),
    StructFromDictPlugin(),
]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register default plugins
    """
    if registry is None:
        registry = st.registry

    for plugin in DEFAULT_PLUGINS:
        registry.register(plugin)
