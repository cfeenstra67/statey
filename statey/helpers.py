import asyncio
import contextlib
import sys
from typing import Callable, Optional, Sequence, Dict, Any

import click

import statey as st


async def refresh(
    graph: "ResourceGraph",
    finalize: bool = False,
    node_cb: Callable[[str], None] = lambda x: None,
    progressbar: bool = False,
    registry: Optional["Registry"] = None,
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
    graph: "ResourceGraph",
    finalize: bool = False,
    node_cb: Callable[[str], None] = lambda x: None,
    registry: Optional["Registry"] = None,
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
    session: "Session",
    resource_graph: Optional["ResourceGraph"] = None,
    refresh: bool = True,
    migrator: Optional["Migrator"] = None,
    refresh_cb: Callable[[str], None] = lambda x: None,
    refresh_progressbar: bool = False,
    graph_cb: Callable[["ResourceGraph"], None] = lambda x: None,
    registry: Optional["Registry"] = None,
) -> "Plan":
    """
    Run a planning operation, optionally 
    """
    from statey.plan import DefaultMigrator

    if registry is None:
        registry = st.registry

    if migrator is None:
        migrator = DefaultMigrator()

    if refresh and resource_graph is not None:
        await _refresh(resource_graph, False, refresh_cb, refresh_progressbar, registry)

    if resource_graph is not None:
        graph_cb(resource_graph)

    return await migrator.plan(session, resource_graph)


@contextlib.asynccontextmanager
async def async_providers_context(providers: Sequence["Provider"]):
    """
    Set up and tear down all necessary providers using a context manager
    pattern.
    """

    @contextlib.asynccontextmanager
    async def single_wrapper(provider):
        await provider.setup()
        try:
            yield
        finally:
            await provider.teardown()

    async with contextlib.AsyncExitStack() as stack:

        coros = []

        for provider in providers:
            coros.append(stack.enter_async_context(single_wrapper(provider)))

        await asyncio.gather(*coros)

        yield


@contextlib.contextmanager
def providers_context(providers: Sequence["Provider"]):
    """
    Synchronous providers context
    """
    async_ctx = async_providers_context(providers)

    loop = asyncio.get_event_loop()

    loop.run_until_complete(async_ctx.__aenter__())
    try:
        yield
    finally:
        loop.run_until_complete(async_ctx.__aexit__(*sys.exc_info()))


def set_provider_defaults(provider: str, config: Dict[str, Any]) -> None:
    """
    Set the configuration for a provider by calling get_provider() with the given configuraiton.
    """
    @st.registry.register
    class SetProviderDefaults:

        @st.hookimpl
        def get_provider_config(name, params, registry):
            if name != provider:
                return None
            conf = config.copy()
            conf.update(params)
            return conf
