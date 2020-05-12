"""
A Serializer converts a ResourceGraph to a state and vice versa
"""
import abc
from typing import Type

from statey.resource import Registry, ResourceGraph


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
    def load(
        self,
        state_data: bytes,
        registry: Registry,
        graph_class: Type[ResourceGraph] = ResourceGraph,
    ) -> ResourceGraph:
        """
		Construct a StateSnapshot object given bytes
		"""
        raise NotImplementedError
