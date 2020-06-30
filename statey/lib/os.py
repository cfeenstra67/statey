import os
import shutil
from typing import Dict, Any

import statey as st
from statey import (
    Machine,
    transition,
    MachineState,
    NullMachineState,
    MachineResource,
    BoundState,
    S,
    TaskSession,
    task,
)
from statey.syms import types, utils, Object


FileSchema = S.Struct["location" : S.String, "data" : S.String].s

FileType = FileSchema.output_type


class StateyOS:
    """
    A few statey wrappers for OS functions
    """

    class path:
        @utils.native_function
        def realpath(path: str) -> str:
            return os.path.realpath(path)


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
    @task.new
    async def remove_file(path: str) -> types.EmptyType:
        """
        Delete the file
        """
        os.remove(path)
        return {}

    @staticmethod
    @task.new
    async def set_file(data: FileType) -> FileType:
        """
        Create the file
        """
        path = os.path.realpath(data["location"])
        with open(path, "w+") as f:
            f.write(data["data"])
        return dict(data, location=path)

    @staticmethod
    @task.new
    async def rename_file(current: FileType, path: str) -> FileType:
        os.rename(current["location"], path)
        return {"data": current["data"], "location": path}

    def get_expected(
        self, session: TaskSession, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "location": st.map(os.path.realpath, data["location"]),
            "data": data["data"],
        }

    @transition("UP", "UP")
    def modify(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        differ = session.ns.registry.get_differ(current.resource_state.state.type)
        diffconfig = differ.config()
        expected = self.get_expected(session, config.data)

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison("location", compare_realpaths)

        current_literal = current.literal(session.ns.registry)

        diff = differ.diff(current.data, config.data)
        flat = list(diff.flatten(diffconfig))
        if not flat:
            return st.struct[
                "location" : StateyOS.path.realpath(input.location),
                "data" : input.data,
            ]

        paths = {d.path for d in flat}
        loc_changed = ("location",) in paths
        data_changed = ("data",) in paths

        # If location changes only we can just rename the file
        if loc_changed and not data_changed:
            return session["rename_file"] << (
                self.rename_file(current_literal, input.location) >> expected
            )

        if loc_changed:
            rm_file = session["delete_file"] << self.remove_file(
                current_literal.location
            )
            joined_input = st.join(input, rm_file)
            return session["create_file"] << (self.set_file(joined_input) >> expected)

        return session["update_file"] << (self.set_file(input) >> expected)

    @transition("DOWN", "UP")
    def create(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        expected = self.get_expected(session, config.data)
        return session["create_file"] << (self.set_file(input) >> expected)

    @transition("UP", "DOWN")
    def delete(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        current_literal = current.literal(session.ns.registry)
        return session["delete_file"] << self.remove_file(current_literal.location)


DirectorySchema = S.Struct["location" : S.String]


class DirectoryMachine(Machine):
    """
    Simple file state machine
    """

    UP = MachineState("UP", DirectorySchema)
    DOWN = NullMachineState("DOWN")

    async def refresh(self, current: BoundState) -> BoundState:
        state = current.resource_state.state
        if state == self.null_state.state:
            return current
        out = current.data.copy()
        path = os.path.realpath(current.data["location"])
        if not os.path.isdir(path):
            return BoundState(self.null_state, {})
        return BoundState(current.resource_state, {"location": path})

    @staticmethod
    @task.new
    async def remove_dir(path: str) -> types.EmptyType:
        """
        Delete the directory
        """
        shutil.rmtree(path)
        return {}

    @staticmethod
    @task.new
    async def create_dir(path: str) -> str:
        """
        Create the directory
        """
        os.mkdir(path)
        return os.path.realpath(path)

    @staticmethod
    @task.new
    async def rename_dir(from_path: str, to_path: str) -> str:
        """
        Rename the directory from the given path to the given path
        """
        os.rename(from_path, to_path)
        return os.path.realpath(to_path)

    @transition("UP", "UP")
    def modify(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        differ = session.ns.registry.get_differ(current.resource_state.state.type)
        diffconfig = differ.config()
        expected = self.get_expected(session, config.data)

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison("location", compare_realpaths)

        current_literal = current.literal(session.ns.registry)

        diff = differ.diff(current.data, config.data)
        flat = list(diff.flatten(diffconfig))
        if not flat:
            return current_literal

        result_path = session["rename_dir"] << (
            self.rename_dir(current_literal.location, input.location)
        )
        return st.struct["location":result_path] >> {
            "location": st.map(os.path.realpath, config.data["location"])
        }

    @transition("DOWN", "UP")
    def create(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        result_path = session["create_dir"] << self.create_dir(input.location)
        return st.struct["location":result_path] >> {
            "location": st.map(os.path.realpath, config.data["location"])
        }

    @transition("UP", "DOWN")
    def delete(
        self,
        current: BoundState,
        config: BoundState,
        session: TaskSession,
        input: Object,
    ) -> Object:

        current_literal = current.literal(session.ns.registry)
        return session["remove_dir"] << self.remove_dir(current_literal.location)


# Declaring global resources

file_resource = MachineResource("file", FileMachine)

# Resource state factory
File = file_resource.s


directory_resource = MachineResource("directory", DirectoryMachine)

# Resource state factory
Directory = directory_resource.s


RESOURCES = [file_resource, directory_resource]


def register() -> None:
    """
	Register default resources in this module
	"""
    for resource in RESOURCES:
        st.registry.register_resource(resource)
