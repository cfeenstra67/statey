import abc
import dataclasses as dc
from typing import Any, Optional, Mapping

import statey as st
from statey.syms import Object, type_serializers, impl_serializers, session


class ObjectSerializer(abc.ABC):
    """
    Serializes an object
    """

    @abc.abstractmethod
    def serialize(self, obj: Object) -> Any:
        """
        Serialize an object into a JSON-serializable object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> Object:
        """
        Deserialize the given data into an object
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class DefaultObjectSerializer(abc.ABC):
    """
    Default object serializer implementation
    """

    type_serializer: type_serializers.TypeSerializer
    impl_serializer: impl_serializers.ObjectImplementationSerializer

    def serialize(self, obj: Object) -> Any:
        serialized_type = self.type_serializer.serialize(obj._type)
        serialized_impl = self.impl_serializer.serialize(obj._impl)
        return {"type": serialized_type, "impl": serialized_impl}

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> Object:
        typ = self.type_serializer.deserialize(data["type"])
        impl = self.impl_serializer.deserialize(data["impl"], session, objects)
        return Object(impl, typ, session.ns.registry)

    @classmethod
    @st.hookimpl
    def get_object_serializer(
        cls, obj: "Object", registry: "Registry"
    ) -> ObjectSerializer:
        type_serializer = registry.get_type_serializer(obj._type)
        impl_serializer = registry.get_impl_serializer(obj._impl, obj._type)
        return cls(type_serializer, impl_serializer)

    @classmethod
    @st.hookimpl
    def get_object_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectSerializer:
        impl_serializer = registry.get_impl_serializer_from_data(data["impl"])
        type_serializer = registry.get_type_serializer_from_data(data["type"])
        return cls(type_serializer, impl_serializer)


OBJECT_SERIALIZER_CLASSES = [DefaultObjectSerializer]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register all object serializers
    """
    if registry is None:
        registry = st.registry

    for cls in OBJECT_SERIALIZER_CLASSES:
        registry.register(cls)
