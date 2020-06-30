import pluggy

from statey import NS


hookspec = pluggy.HookspecMarker(NS)

hookimpl = pluggy.HookimplMarker(NS)


def set_default_registry(new_registry: "Registry") -> None:
    """
	Configure the default registry to be the given value
	"""
    global registry
    registry = new_registry


def create_plugin_manager() -> pluggy.PluginManager:
    """
	Factory function to create a plugin manager w/ the default namespace
	"""
    return pluggy.PluginManager(NS)


def register_default_plugins(
    encoders: bool = True,
    type_plugins: bool = True,
    semantics: bool = True,
    type_serializers: bool = True,
    resources: bool = True,
    differs: bool = True,
    methods: bool = True
) -> None:
    """
	Convenience method to register all of the default provided hooks for the given
	object types
	"""
    if encoders:
        from statey.syms.encoders import register as register_encoders

        register_encoders()

    if type_plugins:
        from statey.syms.plugins import register as register_type_plugins

        register_type_plugins()

    if semantics:
        from statey.syms.semantics import register as register_semantics

        register_semantics()

    if type_serializers:
        from statey.syms.type_serializers import register as register_serializers

        register_serializers()

    if resources:
        from statey.lib import register as register_resources

        register_resources()

    if differs:
        from statey.syms.diff import register as register_differs

        register_differs()

    if methods:
        from statey.syms.methods import register as register_methods

        register_methods()
