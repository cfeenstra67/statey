from typing import Optional

import pylumi

import statey as st
from statey.ext.pulumi import exc
from statey.ext.pulumi.constants import PULUMI_NS


class PulumiPluginInstaller(st.PluginInstaller):
    """
    Plugin installer using pulumi's native functionality
    """

    def __init__(self) -> None:
        self.ctx = None

    def setup(self) -> None:
        self.ctx = pylumi.Context()
        self.ctx.setup()

    def teardown(self) -> None:
        self.ctx.teardown()
        self.ctx = None

    def install(self, name: str, version: Optional[str] = None) -> None:
        if not version:
            raise exc.PluginVersionRequired(name)
        _, plugin_name = name.split("/", 1)
        self.ctx.install_plugin("resource", plugin_name, version)

    @st.hookimpl
    def get_plugin_installer(
        self, name: str, registry: st.Registry
    ) -> st.PluginInstaller:
        if not name.startswith(PULUMI_NS + "/"):
            return None
        return self


PLUGINS = [PulumiPluginInstaller()]


def register(registry: Optional[st.Registry] = None) -> None:
    """
    Register plugins for this module
    """
    if registry is None:
        registry = st.registry

    for plugin in PLUGINS:
        registry.register(plugin)
