"""
A Serializer implementation that serializes graph to a JSON format.
"""
import json
from typing import Type

from statey.resource import ResourceGraph, Registry
from statey.storage import Serializer


class JSONSerializer(Serializer):
    """
	Default JSON serializer for state data
	"""

    def dump(self, graph: ResourceGraph) -> bytes:
        return json.dumps(graph.to_dict(), indent=4, sort_keys=True).encode()

    def load(
        self,
        state_data: bytes,
        registry: Registry,
        graph_class: Type[ResourceGraph] = ResourceGraph,
    ) -> ResourceGraph:
        if state_data == b"":
            return ResourceGraph(registry)
        data = json.loads(state_data.decode())
        instance = graph_class(registry)
        instance.from_dict(data)
        return instance
