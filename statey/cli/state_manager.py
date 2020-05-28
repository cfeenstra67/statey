import abc
import dataclasses as dc
import json
import os

import statey as st
from statey.cli import exc
from statey.resource import ResourceGraph


class StateManager(abc.ABC):
    """
	A state manager handles reading and writing states to storage
	"""

    @abc.abstractmethod
    def load(self, registry: st.Registry) -> ResourceGraph:
        """
		Load the resource graph from some storage
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def dump(self, graph: ResourceGraph, registry: st.Registry) -> None:
        """
		Store the resource graph
		"""
        raise NotImplementedError


@dc.dataclass(frozen=True)
class FileStateManager(StateManager):
    """
	Simple state manager that just read and writes states to a file
	"""

    path: str

    def load(self, registry: st.Registry) -> ResourceGraph:
        if not os.path.exists(self.path):
            return ResourceGraph()
        with open(self.path) as f:
            graph_dict = json.load(f)
        return ResourceGraph.from_dict(graph_dict, registry)

    def dump(self, graph: ResourceGraph, registry: st.Registry) -> None:
        graph_as_dict = graph.to_dict(registry)
        with open(self.path, "w+") as f:
            json.dump(graph_as_dict, f, indent=2, sort_keys=True)
