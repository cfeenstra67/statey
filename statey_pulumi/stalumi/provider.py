from functools import partial
from typing import Any, Sequence

import _stalumi

from stalumi.utils import PROVIDER_METHODS


class Provider:
    """

    """
    def __init__(self, ctx: "Context", name: str) -> None:
        self.name = name
        self.encoded_name = name.encode()
        self.ctx = ctx

    def __getattr__(self, attr: str) -> Any:
        """

        """
        ctx_attr = 'provider_' + attr

        if ctx_attr in PROVIDER_METHODS:
            method = getattr(_stalumi, ctx_attr)
            return partial(method, self.ctx.encoded_name, self.encoded_name)

        return getattr(super(), attr)

    def __dir__(self) -> Sequence[str]:
        """

        """
        sups = dir(super())
        return sups + ['provider_' + attr for attr in PROVIDER_METHODS]
