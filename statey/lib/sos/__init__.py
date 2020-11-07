from typing import Optional

from .file import File, FileConfigType, FileType, StatType


def register(registry: Optional["Registry"] = None) -> None:
    from . import file

    if registry is None:
        from statey import registry

    file.register(registry)
