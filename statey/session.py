"""
A statey session provides a single interface to work with manipulate ResourceGraph objects
"""
from typing import Optional

from statey import exc


class Session:
    """
    An interactive statey session. This can be used to build resource graphs
    """

    def __init__(self, state: "State"):
        self.state = state

    def graph(self) -> "ResourceGraph":
        """
        Obtain a new ResourceGraph instance for this session
        """
        from statey.resource import ResourceGraph

        return ResourceGraph(self)

    def path(self, resource: "Resource") -> str:  # pylint: disable=no-self-use
        """
        Given a resource snapshot, generate the path to that snapshot.
        """
        return resource.name

    def plan(
        self,
        graph: "ResourceGraph",
        state_graph: Optional["ResourceGraph"] = None,
        refresh: bool = True,
    ) -> "Plan":
        """
        Create a plan to apply the given graph. Optionally pass a state graph as well.
        If no state graph is passed, one will be retrieved from the state using load_state(refresh=refresh).
        The refresh flag is provided as a convenience, and will be passed as the `refresh` argument
        of load_state(). If `state_graph` is provided and `refresh=True`, it will be refreshed using
        state.refresh()
        """
        from statey.plan import Plan

        if graph.session is not self:
            raise exc.ForeignGraphError(
                f"Argument `graph` contained a graph constructed using a different session: "
                f"{repr(graph.session)}. Expected: {repr(self)}."
            )

        if state_graph is not None and state_graph.session is not self:
            raise exc.ForeignGraphError(
                f"Argument `state_graph` contained a graph constructed using a different session: "
                f"{repr(state_graph.session)}. Expected: {repr(self)}."
            )

        with self.state.read_context() as ctx:
            if state_graph is None:
                state_graph = self.state.read(self, ctx, refresh=False)

            # Need to run resolve_partial once on recently read states so that factory
            # values are computed
            if state_graph is not None:
                state_graph = state_graph.resolve_all(partial=True)

            refreshed_state = state_graph
            if refresh and state_graph is not None:
                refreshed_state = self.state.refresh(refreshed_state)
                refreshed_state = refreshed_state.resolve_all(partial=True)

            plan = Plan(
                config_graph=graph, state_graph=refreshed_state, original_state_graph=state_graph,
            )
            plan.build()

            return plan

    def apply(self, plan: "Plan", executor: Optional["PlanExecutor"] = None) -> "ApplyResult":
        """
        Apply the given plan with the given executor, updating the state storage accordingly
        """
        with self.state.write_context() as ctx:
            result = plan.apply(executor)
            state_graph = result.state_graph.resolve_all(lambda field: field.store)
            self.state.write(state_graph, ctx)
        return result
