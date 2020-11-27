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
    providers: bool = True,
    resources: bool = True,
    differs: bool = True,
    methods: bool = True,
    casters: bool = True,
    impl_serializers: bool = True,
    object_serializers: bool = True,
    namespace_serializers: bool = True,
    session_serializers: bool = True,
    state_managers: bool = True,
    setuptools_entrypoints: bool = True,
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

    if impl_serializers:
        from statey.syms.impl_serializers import register as register_impl_serializers

        register_impl_serializers(registry)

    if object_serializers:
        from statey.syms.object_serializers import (
            register as register_object_serializers,
        )

        register_object_serializers(registry)

    if namespace_serializers:
        from statey.syms.namespace_serializers import (
            register as register_ns_serializers,
        )

        register_ns_serializers(registry)

    if session_serializers:
        from statey.syms.session_serializers import (
            register as register_session_serializers,
        )

        register_session_serializers(registry)

    if providers:
        from statey.provider import register as register_providers

        register_providers(registry)

    if resources:
        from statey.lib import register as register_resources

        register_resources(registry)

    if state_managers:
        from statey.state_manager import register as register_state_managers

        register_state_managers(registry)

    if setuptools_entrypoints:
        registry.load_setuptools_entrypoints()
