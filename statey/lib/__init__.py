# Exports
from statey.lib import os


def register(os: bool = True) -> None:
    """
	Register all default library resources
	"""
    if os:
        from statey.lib.os import register

        register()
