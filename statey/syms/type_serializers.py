import abc
import dataclasses as dc
from typing import Any, Type as PyType, Dict, Optional

import statey as st
from statey.syms import types


class TypeSerializer(abc.ABC):
    """
	A type serializer encodoes/decodes a type to/from a JSON-serializable value
	"""

    @abc.abstractmethod
    def serialize(self, type: types.Type) -> Any:
        """
		Serialize the given type to a dictionary or some JSONI-serializable value
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, data: Any) -> types.Type:
        """
		Deserialize the gien data (which should be the output of a serialize() call)
		into a native types.Type
		"""
        raise NotImplementedError


class ValueTypeSerializer(TypeSerializer):
    """
	This serializes simple "value" types like ints, booleans
	"""

    type_cls: PyType[types.Type]
    type_name: str

    def serialize(self, type: types.Type) -> Any:
        return {"type": self.type_name, "nullable": type.nullable}

    def deserialize(self, data: Any) -> types.Type:
        return self.type_cls(nullable=data.get("nullable", False))

    @classmethod
    @st.hookimpl
    def get_type_serializer(
        cls, type: types.Type, registry: "Registry"
    ) -> "TypeSerializer":
        if not isinstance(type, cls.type_cls):
            return None
        return cls()

    @classmethod
    @st.hookimpl
    def get_type_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> "TypeSerializer":
        if data.get("type") != cls.type_name:
            return None
        return cls()


@dc.dataclass(frozen=True)
class IntegerTypeSerializer(ValueTypeSerializer):
    """
	Value type serializer for ints
	"""

    type_cls = types.IntegerType
    type_name = "integer"


@dc.dataclass(frozen=True)
class FloatTypeSerializer(ValueTypeSerializer):
    """
	Value type serializer for floats
	"""

    type_cls = types.FloatType
    type_name = "number"


@dc.dataclass(frozen=True)
class BooleanTypeSerializer(ValueTypeSerializer):
    """
	Value type serializer for booleans
	"""

    type_cls = types.BooleanType
    type_name = "boolean"


@dc.dataclass(frozen=True)
class StringTypeSerializer(ValueTypeSerializer):
    """
	Value type serializer for strings
	"""

    type_cls = types.StringType
    type_name = "string"


@dc.dataclass(frozen=True)
class AnyTypeSerializer(ValueTypeSerializer):
    """
    Value type serializer for Any
    """

    type_cls = types.AnyType
    type_name = "any"


@dc.dataclass(frozen=True)
class ArrayTypeSerializer(TypeSerializer):
    """
	Type serializer for arrays
	"""

    element_serializer: "TypeSerializer"

    def serialize(self, type: types.Type) -> Any:
        element_type = self.element_serializer.serialize(type.element_type)
        return {"type": "array", "items": element_type, "nullable": type.nullable}

    def deserialize(self, data: Any) -> types.Type:
        element_type = self.element_serializer.deserialize(data["items"])
        return types.ArrayType(
            element_type=element_type, nullable=data.get("nullable", False)
        )

    @classmethod
    @st.hookimpl
    def get_type_serializer(
        cls, type: types.Type, registry: "Registry"
    ) -> "TypeSerializer":
        if not isinstance(type, types.ArrayType):
            return None
        element_serializer = registry.get_type_serializer(type.element_type)
        return cls(element_serializer)

    @classmethod
    @st.hookimpl
    def get_type_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> "TypeSerializer":
        if data.get("type") != "array":
            return None
        element_type_data = data["items"]
        element_serializer = registry.get_type_serializer_from_data(element_type_data)
        return cls(element_serializer)


@dc.dataclass(frozen=True)
class MapTypeSerializer(TypeSerializer):
    """
    Type serializer for arrays
    """

    key_serializer: TypeSerializer
    value_serializer: TypeSerializer

    def serialize(self, type: types.Type) -> Any:
        key_type = self.key_serializer.serialize(type.key_type)
        value_type = self.value_serializer.serialize(type.value_type)
        return {
            "type": "object",
            "nullable": type.nullable,
            "keys": key_type,
            "additionalProperties": value_type,
        }

    def deserialize(self, data: Any) -> types.Type:
        key_type = self.key_serializer.deserialize(data.get("keys", {"type": "string"}))
        value_type = self.value_serializer.deserialize(data["additionalProperties"])
        return types.MapType(
            key_type=key_type,
            value_type=value_type,
            nullable=data.get("nullable", False),
        )

    @classmethod
    @st.hookimpl
    def get_type_serializer(
        cls, type: types.Type, registry: "Registry"
    ) -> "TypeSerializer":
        if not isinstance(type, types.MapType):
            return None
        key_serializer = registry.get_type_serializer(type.key_type)
        value_serializer = registry.get_type_serializer(type.value_type)
        return cls(key_serializer, value_serializer)

    @classmethod
    @st.hookimpl
    def get_type_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> "TypeSerializer":
        if data.get("type") != "object" or "additionalProperties" not in data:
            return None
        key_type_data = data.get("keys", {"type": "string"})
        key_serializer = registry.get_type_serializer_from_data(key_type_data)
        value_type_data = data["additionalProperties"]
        value_serializer = registry.get_type_serializer_from_data(value_type_data)
        return cls(key_serializer, value_serializer)


@dc.dataclass(frozen=True)
class StructTypeSerializer(TypeSerializer):
    """
	Type serializer for structs
	"""

    field_serializers: Dict[str, TypeSerializer]

    def serialize(self, type: types.Type) -> Any:
        fields = {}

        field_names = [field.name for field in type.fields]
        ordered_fields = sorted(self.field_serializers, key=field_names.index)

        required = []

        for key in ordered_fields:
            serializer = self.field_serializers[key]
            serialized = serializer.serialize(type[key].type)
            if not serialized.pop("nullable", False):
                required.append(key)
            fields[key] = serialized

        return {
            "type": "object",
            "nullable": type.nullable,
            "properties": fields,
            "required": required,
        }

    def deserialize(self, data: Any) -> types.Type:
        fields = []
        for key, field_type in data.get("properties", {}).items():
            field_type_dict = dict(
                field_type, nullable=key not in data.get("required", [])
            )
            serializer = self.field_serializers[key]
            fields.append(
                types.Field(name=key, type=serializer.deserialize(field_type_dict))
            )
        return types.StructType(
            fields=tuple(fields), nullable=data.get("nullable", False)
        )

    @classmethod
    @st.hookimpl
    def get_type_serializer(
        cls, type: types.Type, registry: "Registry"
    ) -> "TypeSerializer":
        if not isinstance(type, types.StructType):
            return None
        field_serializers = {}
        for field in type.fields:
            field_serializers[field.name] = registry.get_type_serializer(field.type)
        return cls(field_serializers)

    @classmethod
    @st.hookimpl
    def get_type_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> "TypeSerializer":
        if data.get("type") != "object" or "additionalProperties" in data:
            return None
        fields = data.get("properties", {})
        field_serializers = {}
        for key, field_type in fields.items():
            field_type_dict = dict(
                field_type, nullable=key not in data.get("required", [])
            )
            field_serializers[key] = registry.get_type_serializer_from_data(
                field_type_dict
            )
        return cls(field_serializers)


@dc.dataclass(frozen=True)
class NativeFunctionTypeSerializer(TypeSerializer):
    """
    Type serializer for function types
    """

    arg_serializers: Dict[str, TypeSerializer]
    return_type_serializer: TypeSerializer

    def serialize(self, type: types.Type) -> Any:
        fields = []

        field_names = [field.name for field in type.args]
        ordered_fields = sorted(self.arg_serializers, key=field_names.index)

        arg_types = {arg.name: arg.type for arg in type.args}

        for key in ordered_fields:
            serializer = self.arg_serializers[key]
            fields.append({"name": key, "type": serializer.serialize(arg_types[key])})

        return {
            "type": "native_function",
            "nullable": type.nullable,
            "args": fields,
            "return_type": self.return_type_serializer.serialize(type.return_type),
        }

    def deserialize(self, data: Any) -> types.Type:
        fields = []
        for field in data["args"]:
            serializer = self.arg_serializers[field["name"]]
            fields.append(
                types.Field(
                    name=field["name"], type=serializer.deserialize(field["type"])
                )
            )

        return_type = self.return_type_serializer.deserialize(data["return_type"])
        return types.NativeFunctionType(fields, return_type).with_nullable(
            data.get("nullable", False)
        )

    @classmethod
    @st.hookimpl
    def get_type_serializer(
        cls, type: types.Type, registry: "Registry"
    ) -> "TypeSerializer":
        if not isinstance(type, types.NativeFunctionType):
            return None
        field_serializers = {}
        for field in type.args:
            field_serializers[field.name] = registry.get_type_serializer(field.type)
        return_type_serializer = registry.get_type_serializer(type.return_type)
        return cls(field_serializers, return_type_serializer)

    @classmethod
    @st.hookimpl
    def get_type_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> "TypeSerializer":
        if data.get("type") != "native_function":
            return None
        field_serializers = {}
        for field in data["args"]:
            field_serializers[field["name"]] = registry.get_type_serializer_from_data(
                field["type"]
            )
        return_type_serializer = registry.get_type_serializer_from_data(
            data["return_type"]
        )
        return cls(field_serializers, return_type_serializer)


TYPE_SERIALIZER_CLASSES = [
    IntegerTypeSerializer,
    FloatTypeSerializer,
    BooleanTypeSerializer,
    StringTypeSerializer,
    AnyTypeSerializer,
    ArrayTypeSerializer,
    MapTypeSerializer,
    StructTypeSerializer,
    NativeFunctionTypeSerializer,
]


def register(registry: Optional["Registry"] = None) -> None:
    """
	Register default type serializer classes
	"""
    if registry is None:
        registry = st.registry

    for cls in TYPE_SERIALIZER_CLASSES:
        registry.register(cls)
