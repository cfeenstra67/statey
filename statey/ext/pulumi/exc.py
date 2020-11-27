from typing import Sequence, Dict, Any


class PulumiError(Exception):
    """
    Errors originating from statey.ext.pulumi
    """


class PulumiValidationError(PulumiError):
    """
    Validation error originating from pulumi
    """

    def __init__(self, errors: Sequence[Dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(f"Parameter validation failed with errors: {errors}.")


class PluginVersionRequired(PulumiError):
    """
    Error indicating that an attempt was made to install a plugin without a version
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f'Cannot install "{name}" without an explicit version.')
