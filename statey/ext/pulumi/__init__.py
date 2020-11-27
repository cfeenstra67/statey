from typing import Optional

try:
    import pylumi
except ImportError as err:
    raise RuntimeError(
        f"`pylumi` is not installed, this extension module cannot be used."
    ) from err

from statey.ext.pulumi.plugin_installer import PulumiPluginInstaller
from statey.ext.pulumi.provider import PulumiProvider
from statey.ext.pulumi.resource import PulumiResourceMachine


def register(registry: Optional["Registry"] = None) -> None:
    from . import provider, helpers, plugin_installer

    if registry is None:
        from statey import registry

    helpers.register(registry)
    provider.register(registry)
    plugin_installer.register(registry)
