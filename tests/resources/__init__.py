from typing import Optional

import statey as st


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register default resources in this module
    """
    if registry is None:
        registry = st.registry

    from tests.resources import file

    file.register(registry)
