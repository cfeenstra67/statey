import os

import statey as st
from statey.fsm import Machine, transition, MachineState, NullMachineState, MachineResource
from statey.resource import BoundState
from statey.syms import symbols, types
from statey.syms.schemas import builder as S
from statey.task import TaskSession, task


FileSchema = S.Struct[
    "location" : S.String(mapper=os.path.realpath),
    "data" : S.String
].s

FileType = FileSchema.output_type


class FileMachine(Machine):
    """
    Simple file state machine
    """
    UP = MachineState("UP", FileSchema)
    DOWN = NullMachineState("DOWN")

    async def refresh(self, current: BoundState) -> BoundState:
        state = current.resource_state.state
        if state == self.null_state.state:
            return current
        out = current.data.copy()
        if not os.path.isfile(current.data["location"]):
            return BoundState(self.null_state, {})
        out["location"] = os.path.realpath(out["location"])
        with open(out["location"]) as f:
            out["data"] = f.read()
        return BoundState(current.resource_state, out)

    async def finalize(self, current: BoundState) -> BoundState:
        return current.clone(data=dict(current.data, data=""))

    @staticmethod
    @task
    async def remove_file(path: str) -> types.EmptyType:
        """
        Delete the file
        """
        os.remove(path)
        return {}

    @staticmethod
    @task
    async def set_file(data: FileType) -> FileType:
        """
        Create the file
        """
        path = os.path.realpath(data["location"])
        with open(path, "w+") as f:
            f.write(data["data"])
        return dict(data, location=path)

    @transition('UP', 'UP')
    def modify(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:

        differ = session.ns.registry.get_differ(current.resource_state.state.type)
        diffconfig = differ.config()

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison('location', compare_realpaths)

        current_literal = current.literal(session.ns.registry)

        diff = differ.diff(current.data, config.data)
        flat = list(diff.flatten(diffconfig))
        if not flat:
            return input

        paths = {d.path for d in flat}
        if ('location',) in paths:
            rm_file = session["delete_file"] << self.remove_file(current_literal.location)
            joined_input = st.join(input, rm_file)
            return session["create_file"] << (self.set_file(joined_input) >> config.data)

        return session["update_file"] << (self.set_file(input) >> config.data)

    @transition('DOWN', 'UP')
    def create(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:

        return session["create_file"] << (self.set_file(input) >> config.data)

    @transition('UP', 'DOWN')
    def delete(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:

        current_literal = current.literal(session.ns.registry)
        return session["delete_file"] << self.remove_file(current_literal.location)

# Declaring global resources

file_resource = MachineResource('file', FileMachine)

# Resource state factory
File = file_resource.s


RESOURCES = [file_resource]


def register() -> None:
    """
	Register default resources in this module
	"""
    for resource in RESOURCES:
        st.registry.register_resource(resource)
