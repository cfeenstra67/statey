from typing import Sequence

import _stalumi


def get_methods_with_prefix(prefix: str) -> Sequence[str]:
    """

    """
    out = []
    for key in dir(_stalumi):
        if key.startswith(prefix):
            out.append(key)
    return out


CONTEXT_METHODS = get_methods_with_prefix('context_')


PROVIDER_METHODS = get_methods_with_prefix('provider_')
