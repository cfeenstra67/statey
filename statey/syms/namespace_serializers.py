import abc
import dataclasses as dc
from typing import Any, Optional

import marshmallow as ma

import statey as st
from statey.syms import session, py_session


class NamespaceSerializer(abc.ABC):
    """
    Serializes and deserializes namespaces
    """
    @abc.abstractmethod
    def serialize(self, ns: session.Namespace) -> Any:
        """
        Serialize the given namespace to a JSON-serializble object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, data: Any) -> session.Namespace:
        """
        Deserialize an object into a namespace
        """
        raise NotImplementedError


class PythonNamespaceSchema(ma.Schema):
    """
    Schema for python namespace dict output
    """
    name = ma.fields.Str(required=True, default='python_namespace')
    types = ma.fields.Dict(keys=ma.fields.Str(), values=ma.fields.Dict())


@dc.dataclass(frozen=True)
class PythonNamespaceSerializer(NamespaceSerializer):
    """
    Serializes/deserializes a python namespace
    """
    registry: "Registry"

    def serialize(self, ns: session.Namespace) -> Any:
        types = {}
        for key, typ in ns.types.items():
            type_ser = self.registry.get_type_serializer(typ)
            types[key] = type_ser.serialize(typ)
        return PythonNamespaceSchema().dump({'types': types})

    def deserialize(self, data: Any) -> session.Namespace:
        loaded_data = PythonNamespaceSchema().load(data)
        types = {}
        ns = py_session.PythonNamespace(self.registry)
        for key, value in loaded_data['types'].items():
            type_ser = self.registry.get_type_serializer_from_data(value)
            typ = type_ser.deserialize(value)
            ns.new(key, typ)
        return ns

    @classmethod
    @st.hookimpl
    def get_namespace_serializer(cls, ns: session.Namespace, registry: "Registry") -> NamespaceSerializer:
        if not isinstance(ns, py_session.PythonNamespace):
            return None
        return cls(registry)

    @classmethod
    @st.hookimpl
    def get_namespace_serializer_from_data(cls, data: Any, registry: "Registry") -> NamespaceSerializer:
        if not isinstance(data, dict) or data.get('name') != 'py_session':
            return None
        return cls(registry)


NAMESPACE_SERIALIZER_CLASSES = [PythonNamespaceSerializer]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register all namespace serializer classes
    """
    if registry is None:
        registry = st.registry

    for cls in NAMESPACE_SERIALIZER_CLASSES:
        registry.register(cls)
