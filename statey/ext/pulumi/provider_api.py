import dataclasses as dc
from typing import Optional, Any, Dict, Sequence

import statey as st
from statey.ext.pulumi import PulumiProvider
from statey.syms import utils


class APIMixin:
    """
    Common behavior for API classes
    """

    def path(self) -> str:
        if self.parent is None:
            return ""
        return ".".join(filter(None, [self.parent.path(), self.name]))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.path()})"


class ProvidersAPI(APIMixin):
    """
    A simple interface for creating resource states fluently
    """

    def __init__(self, registry: Optional[st.Registry] = None) -> None:
        if registry is None:
            registry = st.registry
        self.registry = registry
        self.parent = None
        self._providers = set()

    def get_provider(self, name: str) -> st.Provider:
        """
        Get the pulumi provider with the given name
        """
        api = ProviderAPI(name, self.registry.get_provider("pulumi/" + name), self)
        self._providers.add(name)
        return api

    def __dir__(self) -> Sequence[str]:
        """

        """
        return dir(super()) + list(self._providers)

    def __getattr__(self, attr: str) -> "API_Provider":
        """

        """
        try:
            return getattr(super(), attr)
        except AttributeError:
            if attr.startswith("__"):
                raise

        return self.get_provider(attr)


providers = ProvidersAPI()


class ProviderAPI(APIMixin):
    """

    """

    def __init__(self, name: str, provider: PulumiProvider, parent: Any) -> None:
        self.name = name
        self.provider = provider
        self.parent = parent
        self.modules = self._build_modules(provider)

    def _build_modules(self, provider: PulumiProvider) -> Dict[str, Dict[str, str]]:
        names = {}

        for resource in self.provider.schema.resources:
            _, mod_name, typ = resource.split(":")
            # Might end up being a bug, but using the full names is ugly :(
            root_mod, *_ = mod_name.split("/", 1)
            names.setdefault(root_mod, {})[typ] = resource

        return names

    def get_module(self, name: str) -> "ModuleAPI":
        """

        """
        if name not in self.modules:
            raise KeyError(name)
        return ModuleAPI(name, self.modules[name], self.provider, self)

    def __dir__(self) -> Sequence[str]:
        """

        """
        return dir(super()) + list(self.modules)

    def __getattr__(self, attr: str) -> "ModuleAPI":
        """

        """
        try:
            return getattr(super(), attr)
        except AttributeError:
            if attr.startswith("__"):
                raise

        return self.get_module(attr)


class ModuleAPI(APIMixin):
    """

    """

    def __init__(
        self,
        name: str,
        resources: Dict[str, Dict[str, str]],
        provider: PulumiProvider,
        parent: Any,
    ) -> None:
        self.name = name
        self.provider = provider
        self.parent = parent
        self.resources = resources

    def get_resource(self, name: str) -> st.BoundState:
        """

        """
        if name not in self.resources:
            raise KeyError(name)
        resource_name = self.resources[name]
        resource = self.provider.get_resource(resource_name)
        return resource

    def __dir__(self) -> Sequence[str]:
        """

        """
        return dir(super()) + list(self.resources)

    def __getattr__(self, attr: str) -> "ProviderAPI":
        """

        """
        try:
            return getattr(super(), attr)
        except AttributeError:
            if attr.startswith("__"):
                raise

        return self.get_resource(attr)
