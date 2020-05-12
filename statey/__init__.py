import pluggy


PLUGGY_NS = 'statey'

hookspec = pluggy.HookspecMarker(PLUGGY_NS)

hookimpl = pluggy.HookimplMarker(PLUGGY_NS)


def create_plugin_manager() -> pluggy.PluginManager:
	"""
	Factory function to create a plugin manager w/ the default namespace
	"""
	return pluggy.PluginManager(PLUGGY_NS)
