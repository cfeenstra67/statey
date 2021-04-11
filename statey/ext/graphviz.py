try:
    import graphviz
except ImportError as err:
    raise RuntimeError(
        f"`graphviz` is not installed, this extension module cannot be used."
    ) from err

import dataclasses as dc
from typing import Dict, Any, Callable, Optional

from graphviz import Digraph

import statey as st
from statey.syms import utils


class TaskGraphRenderer:
    """
    Defines some methods for customizing how a ResourceTaskGraph is converted
    to a graphviz.Digraph
    """

    def node_key(self, key: str) -> str:
        return key.replace(":", "/")

    def node_args(self, key: str) -> Dict[str, Any]:
        return {}

    def edge_args(
        self, from_key: str, to_key: str, exists: bool, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        color = "green"
        kws = {}
        if not exists:
            color = "red"
            kws["constraint"] = "false"
        elif data.get("optional"):
            color = "gold2"
        return {"color": color, **kws}


def get_graphviz_graph(
    task_graph: st.ResourceTaskGraph,
    renderer: TaskGraphRenderer = TaskGraphRenderer(),
    task_filter: Optional[Callable[[str, st.Task], bool]] = None,
    **kwargs: Dict[str, Any],
) -> Digraph:
    """
    Get a graphviz Digraph from the given task graph that can be rendered into
    an image
    """
    dot = Digraph(**kwargs)

    original_task_graph = task_graph.original_task_graph
    real_task_graph = task_graph.task_graph
    if original_task_graph is None:
        original_task_graph = real_task_graph

    if task_filter is not None:
        keep = set()

        for node in real_task_graph.nodes:
            task = task_graph.get_task(node)
            if task_filter(node, task):
                keep.add(node)

        utils.subgraph_retaining_dependencies(original_task_graph, keep)
        utils.subgraph_retaining_dependencies(real_task_graph, keep)

    for node in original_task_graph.nodes:
        args = renderer.node_args(node)
        dot.node(renderer.node_key(node), **args)

    for node_from, node_to in original_task_graph.edges:
        exists = (node_from, node_to) in real_task_graph.edges
        data = original_task_graph.edges[node_from, node_to]
        args = renderer.edge_args(node_from, node_to, exists, data)
        dot.edge(renderer.node_key(node_from), renderer.node_key(node_to), **args)

    return dot
