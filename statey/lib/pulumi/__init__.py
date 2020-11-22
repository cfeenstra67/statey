from typing import Optional

try:
    import pylumi
except ImportError as err:
    raise RuntimeError(f'`pylumi` is not installed, this extension module cannot be used.')


def register(registry: Optional["Registry"] = None) -> None:
    from . import provider

    if registry is None:
        from statey import registry

    provider.register(registry)
