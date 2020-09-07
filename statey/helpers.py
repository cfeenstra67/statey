from typing import Callable, Optional

import click

import statey as st
from statey.plan import Migrator, Plan, DefaultMigrator
from statey.registry import Registry
from statey.resource import ResourceGraph
from statey.syms.session import Session


async def refresh(
    graph: ResourceGraph,
    finalize: bool = False,
    node_cb: Callable[[str], None] = lambda x: None,
    progressbar: bool = False,
    registry: Optional[Registry] = None
) -> None:
    """
    Refresh a graph, optionally with a callback
    """
    if registry is None:
        registry = st.registry

    if progressbar:
        return await refresh_with_progressbar(graph, finalize, node_cb, registry)

    async for key in graph.refresh(registry, finalize):
        node_cb(key)


# Alias to avoid naming issues in plan()
_refresh = refresh


async def refresh_with_progressbar(
    graph: ResourceGraph,
    finalize: bool = False,
    node_cb: Callable[[str], None] = lambda x: None,
    registry: Optional[Registry] = None
) -> None:
    """
    refresh with a printed progress bar
    """
    with click.progressbar(
        length=len(graph.graph.nodes),
        label=click.style("Refreshing state...", fg="yellow"),
    ) as bar:

        def cb(key):
            bar.update(1)
            node_cb(key)

        await refresh(graph, finalize, cb, False, registry)


async def plan(
    session: Session,
    resource_graph: Optional[ResourceGraph] = None,
    refresh: bool = True,
    migrator: Optional[Migrator] = None,
    refresh_cb: Callable[[str], None] = lambda x: None,
    refresh_progressbar: bool = False,
    graph_cb: Callable[[ResourceGraph], None] = lambda x: None,
    registry: Optional[Registry] = None
) -> Plan:
    """
    Run a planning operation, optionally 
    """
    if registry is None:
        registry = st.registry

    if migrator is None:
        migrator = DefaultMigrator()

    if refresh and resource_graph is not None:
        await _refresh(resource_graph, False, refresh_cb, refresh_progressbar, registry)

    if resource_graph is not None:
        graph_cb(resource_graph)

    return await migrator.plan(session, resource_graph)
