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
        return self.type_cls(nullable=data["nullable"])

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
    type_name = "float"


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
class ArrayTypeSerializer(TypeSerializer):
    """
	Type serializer for arrays
	"""

    element_serializer: "TypeSerializer"

    def serialize(self, type: types.Type) -> Any:
        element_type = self.element_serializer.serialize(type.element_type)
        return {
            "type": "array",
            "nullable": type.nullable,
            "element_type": element_type,
        }

    def deserialize(self, data: Any) -> types.Type:
        element_type = self.element_serializer.deserialize(data["element_type"])
        return types.ArrayType(element_type=element_type, nullable=data["nullable"])

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
        element_type_data = data["element_type"]
        element_serializer = registry.get_type_serializer_from_data(element_type_data)
        return cls(element_serializer)


@dc.dataclass(frozen=True)
class StructTypeSerializer(TypeSerializer):
    """
	Type serializer for structs
	"""

    field_serializers: Dict[str, TypeSerializer]

    def serialize(self, type: types.Type) -> Any:
        fields = []

        field_names = [field.name for field in type.fields]
        ordered_fields = sorted(self.field_serializers, key=field_names.index)

        for key in ordered_fields:
            serializer = self.field_serializers[key]
            fields.append({"name": key, "type": serializer.serialize(type[key].type)})

        return {"type": "struct", "nullable": type.nullable, "fields": fields}

    def deserialize(self, data: Any) -> types.Type:
        fields = []
        for field in data["fields"]:
            serializer = self.field_serializers[field["name"]]
            fields.append(
                types.Field(
                    name=field["name"], type=serializer.deserialize(field["type"])
                )
            )
        return types.StructType(fields=tuple(fields), nullable=data["nullable"])

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
        if data.get("type") != "struct":
            return None
        fields = data["fields"]
        field_serializers = {}
        for field in data["fields"]:
            field_serializers[field["name"]] = registry.get_type_serializer_from_data(
                field["type"]
            )
        return cls(field_serializers)


TYPE_SERIALIZER_CLASSES = [
    IntegerTypeSerializer,
    FloatTypeSerializer,
    BooleanTypeSerializer,
    StringTypeSerializer,
    ArrayTypeSerializer,
    StructTypeSerializer,
]


def register(registry: Optional["Registry"] = None) -> None:
    """
	Replace default encoder with encoders defined here
	"""
    if registry is None:
        registry = st.registry

    for cls in TYPE_SERIALIZER_CLASSES:
        registry.register(cls)
