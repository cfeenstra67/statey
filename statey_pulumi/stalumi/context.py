from functools import partial
from typing import Any, Sequence

import _stalumi

from stalumi.provider import Provider
from stalumi.utils import CONTEXT_METHODS


class Context:
    """

    """
    def __init__(self, name: str) -> None:
        self.name = name
        self.encoded_name = name.encode()

    def __enter__(self) -> Any:
        """

        """
        _stalumi.context_setup(self.encoded_name)
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        """

        """
        _stalumi.context_teardown(self.encoded_name)

    def __getattr__(self, attr: str) -> Any:
        """

        """
        ctx_attr = 'context_' + attr

        if ctx_attr in CONTEXT_METHODS:
            method = getattr(_stalumi, ctx_attr)
            return partial(method, self.encoded_name)

        return object.__getattribute__(self, attr)

    def __dir__(self) -> Sequence[str]:
        """

        """
        sups = dir(super())
        return sups + ['context_' + attr for attr in CONTEXT_METHODS]

    def Provider(self, name: str) -> Provider:
        """

        """
        return Provider(self, name)
