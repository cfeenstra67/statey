from typing import Optional

import pluggy

from statey import NS


hookspec = pluggy.HookspecMarker(NS)

hookimpl = pluggy.HookimplMarker(NS)


def create_plugin_manager() -> pluggy.PluginManager:
    """
	Factory function to create a plugin manager w/ the default namespace
	"""
    return pluggy.PluginManager(NS)


def register_default_plugins(
    registry: Optional["Registry"] = None,
    encoders: bool = True,
    type_plugins: bool = True,
    semantics: bool = True,
    type_serializers: bool = True,
    resources: bool = True,
    differs: bool = True,
    methods: bool = True,
    casters: bool = True,
    clear_cache: bool = True,
) -> None:
    """
	Convenience method to register all of the default provided hooks for the given
	object types
	"""
    if registry is None:
        from statey import registry

    if encoders:
        from statey.syms.encoders import register as register_encoders

        register_encoders(registry)

    if type_plugins:
        from statey.syms.plugins import register as register_type_plugins

        register_type_plugins(registry)

    if semantics:
        from statey.syms.semantics import register as register_semantics

        register_semantics(registry)

    if type_serializers:
        from statey.syms.type_serializers import register as register_serializers

        register_serializers(registry)

    if differs:
        from statey.syms.diff import register as register_differs

        register_differs(registry)

    if casters:
        from statey.syms.casters import register as register_casters

        register_casters(registry)

    if methods:
        from statey.syms.methods import register as register_methods

        register_methods(registry)

    if resources:
        from statey.lib import register as register_resources

        register_resources(registry)

    if clear_cache:
        from statey.registry import RegistryCachingWrapper

        if isinstance(registry, RegistryCachingWrapper):
            registry.clear_cache()
