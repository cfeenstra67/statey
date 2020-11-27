import abc
from typing import Optional


class PluginInstaller(abc.ABC):
    """
    Implements installation functionality for 
    """

    def setup(self) -> None:
        """
        Perform any necessary setup before installing plugins
        """

    def teardown(self) -> None:
        """
        Tear down any resource created in setup()
        """

    @abc.abstractmethod
    def install(self, name: str, version: Optional[str] = None) -> None:
        """
        Install a plugin with the given name and optionally version. Version strings must
        be exact, not ranges.
        """
        raise NotImplementedError
