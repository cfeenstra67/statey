import warnings
from typing import Optional

# Pulumi is an optional dependency, allow this to fail if `pylumi`
# isn't installed.
try:
    from statey.ext import pulumi
except RuntimeError:
    warnings.warn(
        "`pylumi` is not installed, pulumi providers and resources "
        "will not be usable.",
        RuntimeWarning,
    )


def register(
    registry: Optional["Registry"] = None,
    pulumi: Optional[bool] = None,
) -> None:
    """
    Register all default library resources
    """
    if registry is None:
        from statey import registry

    ## Register pulumi plugins only if dependencies are installed
    if pulumi is None:
        try:
            import statey.ext.pulumi
        except RuntimeError:
            pulumi = False
        else:
            pulumi = True

    if pulumi:
        from statey.ext.pulumi import register as register_pulumi

        register_pulumi(registry)
