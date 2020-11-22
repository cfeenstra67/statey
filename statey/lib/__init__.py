from typing import Optional

# Exports
from statey.lib import sos, pulumi

# try:
#     from statey.lib import aws
# except RuntimeError:
#     pass


def register(
    registry: Optional["Registry"] = None,
    os: bool = True,
    aws: Optional[bool] = None,
    pulumi: Optional[bool] = None,
) -> None:
    """
    Register all default library resources
    """
    if registry is None:
        from statey import registry

    if os:
        from statey.lib.sos import register as register_os

        register_os(registry)

    ## Register pulumi plugins only if dependencies are installed
    if pulumi is None:
        try:
            import statey.lib.pulumi
        except RuntimeError:
            pulumi = False
        else:
            pulumi = True

    if pulumi:
        from statey.lib.pulumi import register as register_pulumi

        register_pulumi(registry)

    # # By default, register AWS resources only if dependencies are installed
    # if aws is None:
    #     try:
    #         import statey.lib.aws
    #     except RuntimeError:
    #         aws = False
    #     else:
    #         aws = True

    # if aws:
    #     from statey.lib.aws import register as register_aws

    #     register_aws(registry)
