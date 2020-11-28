import abc
import dataclasses as dc
from functools import lru_cache
from typing import Dict, Any, Optional, Callable, Sequence

import pluggy

from statey import exc, NS
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

    #   @abc.abstractmethod
    #   def get_resource(self, name: str) -> "Resource":
    #       """
    # Get the resource with the given name
    # """
    #       raise NotImplementedError

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
    def get_impl_serializer(
        self, impl: "ObjectImplementation", type: types.Type
    ) -> "ObjectImplementationSerializer":
        """
        Get an object implementation serializer for the given implementation
        and type
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_impl_serializer_from_data(
        self, data: Any
    ) -> "ObjectImplementationSerializer":
        """
        Get an object implementation serializer from the given data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_object_serializer(self, obj: "Object") -> "ObjectSerializer":
        """
        Get an object serializer for the given object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_object_serializer_from_data(self, data: Any) -> "ObjectSerializer":
        """
        Get an object serializer from the given data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_session_serializer(self, session: "Session") -> "SessionSerializer":
        """
        Get a session serializer for the given session
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_session_serializer_from_data(self, data: Any) -> "SessionSerializer":
        """
        Get a session serializer from the given data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_namespace_serializer(self, ns: "Namespace") -> "NamespaceSerializer":
        """
        Get a namespace serializer for the given namespace
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_namespace_serializer_from_data(self, data: Any) -> "NamespaceSerializer":
        """
        Get a namespace serializer from the given data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_provider(
        self, name: str, params: Optional[Dict[str, Any]] = None
    ) -> "Provider":
        """
        Get a provider with the given name and params
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_state_manager(self) -> "StateManager":
        """
        Get a state manager to use for the current module
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_plugin_installer(self, name: str) -> "PluginInstaller":
        """
        Get a plugin installer for a given plugin name
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
        Hook to create an objecxrt from an arbitrary value
        """

    @hookspec(firstresult=True)
    def get_caster(
        self, from_type: types.Type, to_type: types.Type, registry: Registry
    ) -> "Caster":
        """
        Hook to get a caster object to cast from one type to another
        """

    @hookspec(firstresult=True)
    def get_impl_serializer(
        self, impl: "ObjectImplementation", type: types.Type, registry: Registry
    ) -> "ObjectImplementationSerializer":
        """
        Hook to get an implementation serializer from an implementation and type
        """

    @hookspec(firstresult=True)
    def get_impl_serializer_from_data(
        self, data: Any, registry: Registry
    ) -> "ObjectImplementationSerializer":
        """
        Hook to get an impl serializer from data
        """

    @hookspec(firstresult=True)
    def get_object_serializer(
        self, obj: "Object", registry: Registry
    ) -> "ObjectSerializer":
        """
        Hook to get an object serializer from an object
        """

    @hookspec(firstresult=True)
    def get_object_serializer_from_data(
        self, data: Any, registry: Registry
    ) -> "ObjectSerializer":
        """
        Hook to get an object serializer from data
        """

    @hookspec(firstresult=True)
    def get_session_serializer(
        self, session: "Session", registry: Registry
    ) -> "SessionSerializer":
        """
        Hook to get a session serializer from a session
        """

    @hookspec(firstresult=True)
    def get_session_serializer_from_data(
        self, data: Any, registry: Registry
    ) -> "SessionSerializer":
        """
        Hook to get a session serializer from data
        """

    @hookspec(firstresult=True)
    def get_namespace_serializer(
        self, ns: "Namespace", registry: Registry
    ) -> "NamespaceSerializer":
        """
        Hook to get a namespace serializer from a namespace
        """

    @hookspec(firstresult=True)
    def get_namespace_serializer_from_data(
        self, data: Any, registry: Registry
    ) -> "NamespaceSerializer":
        """
        Hook to get a namespace serializer from data
        """

    @hookspec(firstresult=True)
    def get_provider(
        self, name: str, params: Dict[str, Any], registry: Registry
    ) -> "Provider":
        """
        Hook to get a provider from a name and params
        """

    @hookspec(firstresult=True)
    def get_provider_config(
        self, name: str, params: Dict[str, Any], registry: Registry
    ) -> Dict[str, Any]:
        """
        Hook to modify or set a default provider configuration
        """

    @hookspec(firstresult=True)
    def get_state_manager(self, registry: Registry) -> "StateManager":
        """
        Hook to fetch a state manager to use for planning operations
        """

    @hookspec(firstresult=True)
    def get_plugin_installer(self, name: str, registry: Registry) -> "PluginInstaller":
        """
        Hook to fetch a plugin installer
        """

    @hookspec(historic=True)
    def register(self, plugin: Any, registry: Registry) -> None:
        """
        Hook called before a plugin is registeed to add additional side effects.
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
        self.pm.hook.register.call_historic(kwargs=dict(plugin=plugin, registry=self))

    def load_setuptools_entrypoints(self) -> None:
        """
        Automatically register plugins via setuptools entry points
        """
        self.pm.load_setuptools_entrypoints(NS)

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

    @lru_cache(maxsize=1000)
    def get_encoder(self, type: types.Type, serializable: bool = False) -> "Encoder":
        handled = self.pm.hook.get_encoder(
            type=type, registry=self, serializable=serializable
        )
        if handled is None:
            raise exc.NoEncoderFound(type, serializable)
        return handled

    @lru_cache(maxsize=1000)
    def get_semantics(self, type: types.Type) -> "Semantics":
        handled = self.pm.hook.get_semantics(type=type, registry=self)
        if handled is None:
            raise exc.NoSemanticsFound(type)
        return handled

    @lru_cache(maxsize=1000)
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

    @lru_cache(maxsize=1000)
    def get_differ(self, type: types.Type) -> "Differ":
        handled = self.pm.hook.get_differ(type=type, registry=self)
        if handled is None:
            raise exc.NoDifferFound(type)
        return handled

    @lru_cache(maxsize=1000)
    def get_differ(self, type: types.Type) -> "Differ":
        handled = self.pm.hook.get_differ(type=type, registry=self)
        if handled is None:
            raise exc.NoDifferFound(type)
        return handled

    @lru_cache(maxsize=1000)
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

    def get_impl_serializer(
        self, impl: "ObjectImplementation", type: types.Type
    ) -> "ObjectImplementationSerializer":
        handled = self.pm.hook.get_impl_serializer(impl=impl, type=type, registry=self)
        if handled is None:
            raise exc.NoObjectImplementationSerializerFoundForImpl(impl, type)
        return handled

    @lru_cache(maxsize=1000)
    def get_caster(self, from_type: types.Type, to_type: types.Type) -> "Object":
        handled = self.pm.hook.get_caster(
            from_type=from_type, to_type=to_type, registry=self
        )
        if handled is None:
            raise exc.NoCasterFound(from_type, to_type)
        return handled

    def get_object_serializer(self, obj: "Object") -> "ObjectSerializer":
        handled = self.pm.hook.get_object_serializer(obj=obj, registry=self)
        if handled is None:
            raise exc.NoObjectSerializerFoundForObject(obj)
        return handled

    def get_session_serializer(self, session: "Session") -> "SessionSerializer":
        handled = self.pm.hook.get_session_serializer(session=session, registry=self)
        if handled is None:
            raise exc.NoSessionSerializerFoundForSession(session)
        return handled

    def get_namespace_serializer(self, ns: "Namespace") -> "NamespaceSerializer":
        handled = self.pm.hook.get_namespace_serializer(ns=ns, registry=self)
        if handled is None:
            raise exc.NoNamespaceSerializerFoundForNamespace(ns)
        return handled

    def get_impl_serializer_from_data(
        self, data: Any
    ) -> "ObjectImplementationSerializer":
        handled = self.pm.hook.get_impl_serializer_from_data(data=data, registry=self)
        if handled is None:
            raise exc.NoObjectImplementationSerializerFoundForData(data)
        return handled

    def get_object_serializer_from_data(self, data: Any) -> "ObjectSerializer":
        handled = self.pm.hook.get_object_serializer_from_data(data=data, registry=self)
        if handled is None:
            raise exc.NoObjectSerializerFoundForData(data)
        return handled

    def get_session_serializer_from_data(self, data: Any) -> "SessionSerializer":
        handled = self.pm.hook.get_session_serializer_from_data(
            data=data, registry=self
        )
        if handled is None:
            raise exc.NoSessionSerializerFoundForData(data)
        return handled

    def get_namespace_serializer_from_data(self, data: Any) -> "NamespaceSerializer":
        handled = self.pm.hook.get_namespace_serializer_from_data(
            data=data, registry=self
        )
        if handled is None:
            raise exc.NoNamespaceSerializerFoundForData(data)
        return handled

    def get_provider(
        self, name: str, params: Optional[Dict[str, Any]] = None
    ) -> "Provider":
        if params is None:
            params = {}
        config = self.pm.hook.get_provider_config(
            name=name, params=params, registry=self
        )
        if config is None:
            config = {}
        handled = self.pm.hook.get_provider(name=name, params=config, registry=self)
        if handled is None:
            raise exc.NoProviderFound(name, params)
        return handled

    def get_state_manager(self) -> "StateManager":
        manager = self.pm.hook.get_state_manager(registry=self)
        if manager is None:
            raise exc.NoStateManagerFound()
        return manager

    def get_plugin_installer(self, name: str) -> "PluginInstaller":
        installer = self.pm.hook.get_plugin_installer(name=name, registry=self)
        if installer is None:
            raise exc.NoPluginInstallerFound(name)
        return installer


def create_registry() -> Registry:
    """
    Create a registry using the default implementation
    """
    return HookBasedRegistry()
