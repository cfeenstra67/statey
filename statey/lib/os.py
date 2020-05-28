import os

import statey as st
from statey.resource import Resource, KnownStates, State, NullState, BoundState
from statey.syms import symbols, types
from statey.task import TaskSession, task


FileType = types.StructType(
    [
        types.StructField("location", types.StringType(False)),
        types.StructField("data", types.StringType(False)),
    ],
    False,
)


# Tasks
@task
async def set_file(data: FileType) -> FileType:
    """
	Create the file
	"""
    path = os.path.realpath(data["location"])
    with open(path, "w+") as f:
        f.write(data["data"])
    return dict(data, location=path)


@task
async def remove_file(path: str) -> types.EmptyType:
    """
	Delete the file
	"""
    os.remove(path)
    return {}


class FileResource(Resource):
    """
	Represents a file on the file system.
	"""

    class States(KnownStates):
        UP = State("UP", FileType)
        DOWN = NullState("DOWN")

    def __init__(self, name: str) -> None:
        self._name = name
        super().__init__()

    @property
    def name(self) -> str:
        return self._name

    async def refresh(self, current: BoundState) -> BoundState:
        state = current.resource_state.state
        if state == self.s.null_state.state:
            return current
        out = current.data.copy()
        if not os.path.isfile(current.data["location"]):
            return BoundState(self.s.null_state, {})
        out["location"] = os.path.realpath(out["location"])
        with open(out["location"]) as f:
            out["data"] = f.read()
        return BoundState(current.resource_state, out)

    def plan(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:

        current_state = current.resource_state.state
        current_data = current.data
        current_literal = current.literal(session.ns.registry)

        config_state = config.resource_state.state
        config_data = config.data
        config_literal = current.literal(session.ns.registry)

        if config_state.name == "UP" and not isinstance(
            config_data["location"], symbols.Unknown
        ):
            config_data["location"] = os.path.realpath(config_data["location"])

        def join(x, *args):
            return symbols.Function(
                func=lambda x, *args: x, args=(x, *args), semantics=x.semantics
            )

        def finalize(output):
            if config_state.name == "UP":
                graph_ref = output.map(lambda x: dict(x, data=""))
                return output, graph_ref
            return output

        # No change, just return the input ref (which will be of the correct type).
        # Also, because `current` will always be fully resolved we don't have to
        # worry too much about a very deep '==' comparison
        if current_state == config_state and current_data == config_data:
            return finalize(current_literal)

        # UP -> DOWN
        if config_state == self.s.null_state.state:
            ref = session["delete_file"] << remove_file(
                current_literal.location
            ).expecting(config_data)
            return finalize(ref)
        # DOWN -> UP
        if current_state == self.s.null_state.state:
            ref = session["create_file"] << set_file(input).expecting(config_data)
            return finalize(ref)

        # UP -> UP if data different
        if current_data["location"] == config_data["location"]:
            ref = session["update_file"] << set_file(input).expecting(config_data)
            return finalize(ref)

        rm_file = session["delete_file"] << remove_file(current_literal.location)
        ref = session["create_file"] << set_file(join(input, rm_file)).expecting(
            config_data
        )
        return finalize(ref)


# Declaring global resources

file_resource = FileResource("file")

# Resource state factory
File = file_resource.s


RESOURCES = [file_resource]


def register() -> None:
    """
	Register default resources in this module
	"""
    for resource in RESOURCES:
        st.registry.register_resource(resource)
