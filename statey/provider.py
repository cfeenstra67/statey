import abc
import dataclasses as dc
from typing import Optional, Dict, Any, Sequence

import marshmallow as ma
import pluggy

import statey as st
from statey import exc
from statey.hooks import hookspec, create_plugin_manager, hookimpl
from statey.task import Task


class ProviderIdSchema(ma.Schema):
    """
    Schema for a ProviderId
    """

    name = ma.fields.Str()
    meta = ma.fields.Dict(keys=ma.fields.Str(), values=ma.fields.Str())


@dc.dataclass(frozen=True)
class ProviderId:
    """
    Unique identifier for a provider
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProviderId":
        data = ProviderIdSchema().load(data)
        return cls(**data)

    name: str
    meta: Dict[str, str] = dc.field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.name, frozenset(self.meta.items())))

    def __iter__(self) -> iter:
        return iter((self.name, self.meta))

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert this ProviderId to a dictionary
        """
        return ProviderIdSchema().dump(self)


class Provider(abc.ABC):
    """
    A provider provides methods for accessing resources and tasks.
    """

    id: ProviderId

    @abc.abstractmethod
    def list_resources(self) -> Sequence[str]:
        """
        List the resource names available in this provider
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_resource(self, name: str) -> "Resource":
        """
        Get the resource with the given name.
        """
        raise NotImplementedError

    async def setup(self) -> None:
        """
        Perform any necessary provider setup before running
        tasks.
        """

    async def teardown(self) -> None:
        """
        Perform any necessary provider teardown after it is
        no longer being used.
        """


class ProviderHooks:
    """
    Defines hooks to power the default provider
    """

    @hookspec(firstresult=True)
    def get_resource(self, name: str, provider: Provider) -> "Resource":
        """
        Analogous to the Provider.get_resource() method
        """


def create_default_provider_plugin_manager() -> pluggy.PluginManager:
    """
    Factory to create a fresh DefaultProvider plugin manager
    """
    mgr = create_plugin_manager()
    mgr.add_hookspecs(ProviderHooks)
    return mgr


@dc.dataclass(frozen=True)
class DefaultProviderHooks:
    """
    Default hookimpls for DefaultProvider, separate class to avoid name collisions.
    """

    provider: "DefaultProvider"

    @hookimpl
    def get_resource(self, name: str, provider: Provider) -> "Resource":
        real_name = provider._get_registered_resource_name(name)
        return provider.pm.get_plugin(real_name)

    @hookimpl
    def register(self, plugin: Any, registry: "Registry") -> None:
        if isinstance(plugin, st.Resource):
            self.provider.register(plugin)

    @hookimpl
    def get_provider(
        self, name: str, params: Dict[str, Any], registry: "Registry"
    ) -> Provider:
        if ProviderId(name, params) != self.provider.id:
            return None
        if not registry.pm.is_registered(self):
            registry.register(self)
        return self.provider


@dc.dataclass(frozen=True)
class DefaultProvider(Provider):
    """
    Hook-based
    """

    id: ProviderId = dc.field(default=ProviderId("default"))
    pm: pluggy.PluginManager = dc.field(
        init=False,
        default_factory=create_default_provider_plugin_manager,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.pm.register(self.hookimpls())
        self.__dict__["_resource_names"] = set()

    def list_resources(self) -> Sequence[str]:
        return sorted(self.__dict__["_resource_names"])

    def get_resource(self, name: str) -> "Resource":
        resource = self.pm.hook.get_resource(name=name, provider=self)
        if resource is None:
            raise exc.ResourceNotFound(name, self)
        return resource

    def _get_registered_resource_name(self, resource_name: str) -> str:
        return f"resource:{resource_name}"

    def register(self, resource: "Resource") -> None:
        self.pm.register(
            resource, name=self._get_registered_resource_name(resource.name)
        )
        self.__dict__["_resource_names"].add(resource.name)

    def hookimpls(self) -> DefaultProviderHooks:
        return DefaultProviderHooks(self)


default_provider = DefaultProvider()


DEFAULT_PROVIDERS = [default_provider]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register default plugins
    """
    if registry is None:
        from statey import registry

    for provider in DEFAULT_PROVIDERS:
        registry.register(provider.hookimpls())
