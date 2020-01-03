"""
A Serializer converts a ResourceGraph to a state and vice versa
"""
import abc

from statey.resource import ResourceGraph


class Serializer(abc.ABC):
    """
	Serialize a StateSnapshot to bytes, and vice versa
	"""

    @abc.abstractmethod
    def dump(self, graph: ResourceGraph) -> bytes:
        """
		Serialize a snapshot to bytes
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, state_data: bytes, session: "Session") -> ResourceGraph:
        """
		Construct a StateSnapshot object given bytes
		"""
        raise NotImplementedError
