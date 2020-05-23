import pluggy

from statey import NS


hookspec = pluggy.HookspecMarker(NS)

hookimpl = pluggy.HookimplMarker(NS)


def set_default_registry(new_registry: 'Registry') -> None:
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
