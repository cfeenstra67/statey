class StorageError(Exception):
    """
	Base class for errors relating to storing or loading resource
	graphs
	"""


class StateLoadError(StorageError):
    """
	Specifically errors relate to loading a graph from storage
	"""


class StateDumpError(StorageError):
    """
	Specifically errors related to dumping a graph to storage
	"""
