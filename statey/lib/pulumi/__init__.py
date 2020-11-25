from typing import Optional

try:
    import pylumi
except ImportError as err:
    raise RuntimeError(
        f"`pylumi` is not installed, this extension module cannot be used."
    )

from statey.lib.pulumi.provider import PulumiProvider
from statey.lib.pulumi.resource import PulumiResourceMachine


def register(registry: Optional["Registry"] = None) -> None:
    from . import provider, helpers

    if registry is None:
        from statey import registry

    helpers.register_meta_finder()
    provider.register(registry)
