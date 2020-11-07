from typing import Optional


def register(registry: Optional["Registry"] = None) -> None:
    from . import plan

    if registry is None:
        from statey import registry

    plan.register(registry)
