from typing import Optional

# Exports
from statey.ext import pulumi


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
