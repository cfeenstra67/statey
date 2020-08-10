import abc
import dataclasses as dc
from functools import lru_cache
from typing import Dict, Any, Optional, Callable, Sequence

import pluggy

from statey import exc
from statey.hooks import hookspec, create_plugin_manager, register_default_plugins
from statey.syms import types, utils


class Registry(abc.ABC):
    """
	A type registry is used to parse annotations into types
	"""

    @abc.abstractmethod
    def get_type(
        self, annotation: Any, meta: Optional[Dict[str, Any]] = None
    ) -> types.Type:
        """
		Parse the given annotation and return a Type. This will properly handle dataclasses
		similarly to how case classes are handled in spark encoding
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def infer_type(self, obj: Any) -> types.Type:
        """
		Attempt to infer the type of `obj`, falling back on Any
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_encoder(self, type: types.Type, serializable: bool = False) -> "Encoder":
        """
		Given a type, return an Encoder instance to encode the type, raising an exc.NoEncoderFound to
		indicate failure
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_semantics(self, type: types.Type) -> "Semantics":
        """
		Given a type, get the semantics to use for objects of that type.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_type_serializer(self, type: types.Type) -> "TypeSerializer":
        """
		Given a type, get the type serializer to turn it into a JSON-serializable value.
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_type_serializer_from_data(self, data: Any) -> "TypeSerializer":
        """
		Given serialized data, get a type serializer that can decode the serialized type of
		into a native Type
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_differ(self, type: types.Type) -> "Differ":
        """
        Given a type, get a differ instance to diff the data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def register_resource(self, resource: "Resource") -> None:
        """
		Register the given resource
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_resource(self, name: str) -> "Resource":
        """
		Get the resource with the given name
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def get_methods(self, type: types.Type) -> "ObjectMethods":
        """
        Get ObjectMethods for the given type
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_object(self, value: Any) -> "Object":
        """
        Get a statey object given a value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_caster(self, from_type: types.Type, to_type: types.Type) -> "Object":
        """
        Get a caster given from_type and to_type
        """
        raise NotImplementedError

    @abc.abstractmethod
    def register(self, plugin: Any) -> None:
        """
        Register a plugin within this registry
        """
        raise NotImplementedError

    def object(self, *args, **kwargs) -> "Object":
        """
        Create an object bound to this registry
        """
        from statey.syms import Object

        return Object(*args, registry=self, **kwargs)


class RegistryHooks:
    """
	Specifies hooks for handling different annotations and converting them to types
	"""

    @hookspec(firstresult=True)
    def get_type(
        self, annotation: Any, registry: Registry, meta: Dict[str, Any]
    ) -> types.Type:
        """
		Handle the given annotation and return a Type, or None to indicate it can't be handled by this hook
		"""

    @hookspec(firstresult=True)
    def infer_type(self, obj: Any, registry: Registry) -> types.Type:
        """
		Infer a type given an object
		"""

    @hookspec(firstresult=True)
    def get_encoder(
        self, type: types.Type, registry: Registry, serializable: bool
    ) -> "Encoder":
        """
		Handle the given type and produce an Encoder instance that can encode values of that type
        `serializable` indicates that encoded values that the Encoder produces should
        always be JSON-serializable
		"""

    @hookspec(firstresult=True)
    def get_semantics(self, type: types.Type, registry: Registry) -> "Semantics":
        """
		Handle the given type and produce a Semantics instance for objects of that type
		"""

    @hookspec(firstresult=True)
    def get_type_serializer(
        self, type: types.Type, registry: Registry
    ) -> "TypeSerializer":
        """
		Handle the given type and produce an TypeSerializer instance that can encode values of that type
		"""

    @hookspec(firstresult=True)
    def get_type_serializer_from_data(
        self, data: Any, registry: Registry
    ) -> "TypeSerializer":
        """
		Handle the given type and produce a TypeSerializer instance for objects of that type
		"""

    @hookspec(firstresult=True)
    def get_differ(self, type: types.Type, registry: Registry) -> "Encoder":
        """
        Handle the given type and produce an Differ instance for diffing values of that type
        """

    @hookspec
    def get_methods(self, type: types.Type, registry: Registry) -> "ObjectMethods":
        """
        Hook to get methods for a particular type
        """

    @hookspec(firstresult=True)
    def get_object(self, value: Any, registry: Registry) -> "Object":
        """
        Hook to create an object from an arbitrary value
        """

    @hookspec(firstresult=True)
    def get_caster(
        self, from_type: types.Type, to_type: types.Type, registry: Registry
    ) -> "Caster":
        """
        Hook to get a caster object to cast from one type to another
        """


def create_registry_plugin_manager():
    """
	Factory function to create the default base plugin manager for DefaultTypeRegistry
	"""
    pm = create_plugin_manager()
    pm.add_hookspecs(RegistryHooks)
    return pm


@dc.dataclass(frozen=True)
class HookBasedRegistry(Registry):
    """
    Registry based on pluggy hooks
    """

    pm: pluggy.PluginManager = dc.field(
        init=False,
        default_factory=create_registry_plugin_manager,
        compare=False,
        repr=False,
    )

    def register(self, plugin: Any) -> None:
        self.pm.register(plugin)

    def get_type(
        self, annotation: Any, meta: Optional[Dict[str, Any]] = None
    ) -> types.Type:
        if meta is None:
            meta = {}
        handled = self.pm.hook.get_type(annotation=annotation, meta=meta, registry=self)
        if handled is None:
            raise exc.NoTypeFound(annotation)
        return handled

    def infer_type(self, obj: Any) -> types.Type:
        handled = self.pm.hook.infer_type(obj=obj, registry=self)
        if handled is not None:
            return handled
        annotation = utils.infer_annotation(obj)
        return self.get_type(annotation)

    def get_encoder(self, type: types.Type, serializable: bool = False) -> "Encoder":
        handled = self.pm.hook.get_encoder(
            type=type, registry=self, serializable=serializable
        )
        if handled is None:
            raise exc.NoEncoderFound(type, serializable)
        return handled

    def get_semantics(self, type: types.Type) -> "Semantics":
        handled = self.pm.hook.get_semantics(type=type, registry=self)
        if handled is None:
            raise exc.NoSemanticsFound(type)
        return handled

    def get_type_serializer(self, type: types.Type) -> "TypeSerializer":
        handled = self.pm.hook.get_type_serializer(type=type, registry=self)
        if handled is None:
            raise exc.NoTypeSerializerFoundForType(type)
        return handled

    def get_type_serializer_from_data(self, data: Any) -> "TypeSerializer":
        handled = self.pm.hook.get_type_serializer_from_data(data=data, registry=self)
        if handled is None:
            raise exc.NoTypeSerializerFoundForData(data)
        return handled

    def get_differ(self, type: types.Type) -> "Differ":
        handled = self.pm.hook.get_differ(type=type, registry=self)
        if handled is None:
            raise exc.NoDifferFound(type)
        return handled

    def _get_registered_resource_name(self, resource_name: str) -> str:
        return f"resource:{resource_name}"

    def register_resource(self, resource: "Resource") -> None:
        self.pm.register(
            resource, name=self._get_registered_resource_name(resource.name)
        )

    def get_resource(self, name: str) -> "Resource":
        resource = self.pm.get_plugin(self._get_registered_resource_name(name))
        if resource is None:
            raise exc.NoResourceFound(name)
        return resource

    def get_differ(self, type: types.Type) -> "Differ":
        handled = self.pm.hook.get_differ(type=type, registry=self)
        if handled is None:
            raise exc.NoDifferFound(type)
        return handled

    def get_methods(self, type: types.Type) -> "ObjectMethods":
        from statey.syms.methods import CompositeObjectMethods

        methods_instances = self.pm.hook.get_methods(type=type, registry=self)
        non_null_methods = [
            methods for methods in methods_instances if methods is not None
        ]
        return CompositeObjectMethods(tuple(non_null_methods))

    def get_object(self, value: Any) -> "Object":
        handled = self.pm.hook.get_object(value=value, registry=self)
        if handled is None:
            raise exc.NoObjectFound(value)
        return handled

    def get_caster(self, from_type: types.Type, to_type: types.Type) -> "Object":
        handled = self.pm.hook.get_caster(
            from_type=from_type, to_type=to_type, registry=self
        )
        if handled is None:
            raise exc.NoCasterFound(from_type, to_type)
        return handled


class RegistryWrapper(Registry):
    """
    Wrapper class to wrap any methods desired simply
    """

    registry: Registry

    @abc.abstractmethod
    def wrap(self, name: str, func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """
        Wrap the given method
        """
        raise NotImplementedError

    def get_type(
        self, annotation: Any, meta: Optional[Dict[str, Any]] = None
    ) -> types.Type:
        return self.wrap("get_type", self.registry.get_type)(annotation, meta)

    def infer_type(self, obj: Any) -> types.Type:
        return self.wrap("infer_type", self.registry.infer_type)(obj)

    def get_encoder(self, type: types.Type, serializable: bool = False) -> "Encoder":
        return self.wrap("get_encoder", self.registry.get_encoder)(type, serializable)

    def get_semantics(self, type: types.Type) -> "Semantics":
        return self.wrap("get_semantics", self.registry.get_semantics)(type)

    def get_type_serializer(self, type: types.Type) -> "TypeSerializer":
        return self.wrap("get_type_serializer", self.registry.get_type_serializer)(type)

    def get_type_serializer_from_data(self, data: Any) -> "TypeSerializer":
        return self.wrap(
            "get_type_serializer_from_data", self.registry.get_type_serializer_from_data
        )(data)

    def get_differ(self, type: types.Type) -> "Differ":
        return self.wrap("get_differ", self.registry.get_differ)(type)

    def register_resource(self, resource: "Resource") -> None:
        return self.wrap("register_resource", self.registry.register_resource)(resource)

    def get_resource(self, name: str) -> "Resource":
        return self.wrap("get_resource", self.registry.get_resource)(name)

    def get_methods(self, type: types.Type) -> "ObjectMethods":
        return self.wrap("get_methods", self.registry.get_methods)(type)

    def get_object(self, value: Any) -> "Object":
        return self.wrap("get_object", self.registry.get_object)(value)

    def get_caster(self, from_type: types.Type, to_type: types.Type) -> "Object":
        return self.wrap("get_caster", self.registry.get_caster)(from_type, to_type)

    def register(self, plugin: Any) -> None:
        return self.wrap("register", self.registry.register)(plugin)


@dc.dataclass(frozen=True)
class RegistryCachingWrapper(RegistryWrapper):
    """
    Wrapper that wraps get_* methods to cache outputs for better performance
    """

    registry: Registry
    maxsize: int = dc.field(default=1_000)
    caches: Dict[str, Any] = dc.field(default_factory=dict)
    cache_methods: Sequence[str] = dc.field(
        default=(
            "get_encoder",
            "get_semantics",
            "get_type_serializer",
            "get_differ",
            "get_methods",
            "get_caster",
        )
    )

    def wrap(self, name: str, func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        if name not in self.cache_methods:
            return func
        if name in self.caches:
            return self.caches[name]
        cache = self.caches[name] = lru_cache(maxsize=self.maxsize)(func)
        return cache

    def clear_cache(self) -> None:
        """
        Clear the current cache
        """
        for value in self.caches.values():
            value.cache_clear()


def create_registry() -> Registry:
    """
    Create a registry using the default implementation
    """
    # Need to make types hashable again in order for this to work
    # return RegistryCachingWrapper(HookBasedRegistry())
    return HookBasedRegistry()
