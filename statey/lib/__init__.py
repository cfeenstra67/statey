from typing import Optional

# Exports
from statey.lib import os


def register(os: bool = True, aws: Optional[bool] = None) -> None:
    """
    Register all default library resources
    """
    if os:
        from statey.lib.os import register as register_os

        register_os()

    # By default, register AWS resources only if dependencies are installed
    if aws is None:
        try:
            import statey.lib.aws
        except RuntimeError:
            aws = False
        else:
            aws = True

    if aws:
        from statey.lib.aws import register as register_aws

        register_aws()
