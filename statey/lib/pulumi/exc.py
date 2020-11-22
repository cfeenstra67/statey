from typing import Sequence, Dict, Any


class PulumiError(Exception):
    """

    """


class PulumiValidationError(PulumiError):
    """

    """
    def __init__(self, errors: Sequence[Dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(f'Parameter validation failed with errors: {errors}.')
