import os
import shutil
from typing import Dict, Any

import statey as st
from statey import (
    Machine,
    transition,
    MachineResource,
    S,
    TaskSession,
    task,
    StateSnapshot,
    StateConfig,
)
from statey.syms import types, utils, Object, impl


FileConfigType = S.Struct["location" : S.String, "data" : S.String].t


StatSchema = S.Struct[
    "mode" : S.Integer,
    "ino" : S.Integer,
    "dev" : S.Integer,
    "nlink" : S.Integer,
    "uid" : S.Integer,
    "gid" : S.Integer,
    "size" : S.Integer,
    "atime" : S.Float,
    "mtime" : S.Float,
    "ctime" : S.Float,
].s


StatType = StatSchema.output_type


FileType = S.Struct["location" : S.String, "data" : S.String, "stat":StatSchema].t


class FileMachine(Machine):
    """
    Simple file state machine
    """

    UP = st.State("UP", FileConfigType, FileType)
    DOWN = st.NullState("DOWN")

    async def refresh(self, current: StateSnapshot) -> StateSnapshot:
        state = current.state.state
        if state == self.null_state.state:
            return current
        if not os.path.isfile(current.data["location"]):
            return StateSnapshot({}, self.null_state)
        data = self.get_file_info(current.data["location"])
        return StateSnapshot(data, current.state)

    async def finalize(self, current: StateSnapshot) -> StateSnapshot:
        return StateSnapshot(dict(current.data, data=""), current.state)

    @staticmethod
    def get_file_info(path: str) -> FileType:
        location = os.path.realpath(path)
        with open(location) as f:
            data = f.read()

        stat_info = os.stat(path)
        return {
            "location": location,
            "data": data,
            "stat": {
                "mode": stat_info.st_mode,
                "ino": stat_info.st_ino,
                "dev": stat_info.st_dev,
                "nlink": stat_info.st_nlink,
                "uid": stat_info.st_uid,
                "gid": stat_info.st_gid,
                "size": stat_info.st_size,
                "atime": stat_info.st_atime,
                "mtime": stat_info.st_mtime,
                "ctime": stat_info.st_ctime,
            },
        }

    @staticmethod
    def get_file_expected(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "location": st.map(os.path.realpath, data["location"]),
            "data": data["data"],
            "stat": st.Object(impl.Unknown(return_type=StatType)),
        }

    @task.new
    async def remove_file(self, path: str) -> types.EmptyType:
        """
        Delete the file
        """
        os.remove(path)
        return {}

    @task.new
    async def set_file(self, data: FileConfigType) -> FileType:
        """
        Set the file's contents
        """
        path = os.path.realpath(data["location"])
        with open(path, "w+") as f:
            f.write(data["data"])
        return self.get_file_info(path)

    @task.new
    async def rename_file(self, from_path: str, to_path: str) -> FileType:
        from_path = os.path.realpath(from_path)
        to_path = os.path.realpath(to_path)
        os.rename(from_path, to_path)
        return self.get_file_info(to_path)

    @transition("UP", "UP")
    def modify(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession,
        input: Object,
    ) -> Object:

        differ = session.ns.registry.get_differ(current.state.input_type)
        diffconfig = differ.config()
        expected = self.get_file_expected(config.data)

        def compare_realpaths(x, y):
            return os.path.realpath(x) == os.path.realpath(y)

        diffconfig.set_comparison("location", compare_realpaths)

        current_literal = current.obj(session.ns.registry)

        diff = differ.diff(current.data, config.data)
        flat = list(diff.flatten(diffconfig))
        if not flat:
            return current.obj(session.ns.registry)

        paths = {d.path for d in flat}
        loc_changed = ("location",) in paths
        data_changed = ("data",) in paths

        # If location changes only we can just rename the file
        if loc_changed and not data_changed:
            return session["rename_file"] << (
                self.rename_file(current_literal.location, input.location) >> expected
            )

        if loc_changed:
            session["delete_file"] << self.remove_file(current_literal.location)
            return session["create_file"] << (self.set_file(input) >> expected)

        return session["update_file"] << (self.set_file(input) >> expected)

    @transition("DOWN", "UP")
    def create(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession,
        input: Object,
    ) -> Object:

        expected = self.get_file_expected(config.data)
        return session["create_file"] << (self.set_file(input) >> expected)

    @transition("UP", "DOWN")
    def delete(
        self,
        current: StateSnapshot,
        config: StateConfig,
        session: TaskSession,
        input: Object,
    ) -> Object:

        current_literal = current.obj(session.ns.registry)
        session["delete_file"] << self.remove_file(current_literal.location)
        return st.Object({})


# DirectorySchema = S.Struct["location" : S.String].s


# class DirectoryMachine(Machine):
#     """
#     Simple file state machine
#     """

#     UP = MachineState("UP", DirectorySchema)
#     DOWN = NullMachineState("DOWN")

#     async def refresh(self, current: BoundState) -> BoundState:
#         state = current.resource_state.state
#         if state == self.null_state.state:
#             return current
#         out = current.data.copy()
#         path = os.path.realpath(current.data["location"])
#         if not os.path.isdir(path):
#             return BoundState(self.null_state, {})
#         return BoundState(current.resource_state, {"location": path})

#     @staticmethod
#     @task.new
#     async def remove_dir(path: str) -> types.EmptyType:
#         """
#         Delete the directory
#         """
#         shutil.rmtree(path)
#         return {}

#     @staticmethod
#     @task.new
#     async def create_dir(path: str) -> str:
#         """
#         Create the directory
#         """
#         os.mkdir(path)
#         return os.path.realpath(path)

#     @staticmethod
#     @task.new
#     async def rename_dir(from_path: str, to_path: str) -> str:
#         """
#         Rename the directory from the given path to the given path
#         """
#         os.rename(from_path, to_path)
#         return os.path.realpath(to_path)

#     @transition("UP", "UP")
#     def modify(
#         self,
#         current: BoundState,
#         config: BoundState,
#         session: TaskSession,
#         input: Object,
#     ) -> Object:

#         differ = session.ns.registry.get_differ(current.resource_state.state.type)
#         diffconfig = differ.config()
#         expected = self.get_expected(session, config.data)

#         def compare_realpaths(x, y):
#             return os.path.realpath(x) == os.path.realpath(y)

#         diffconfig.set_comparison("location", compare_realpaths)

#         current_literal = current.literal(session.ns.registry)

#         diff = differ.diff(current.data, config.data)
#         flat = list(diff.flatten(diffconfig))
#         if not flat:
#             return current_literal

#         result_path = session["rename_dir"] << (
#             self.rename_dir(current_literal.location, input.location)
#         )
#         return st.struct["location":result_path] >> {
#             "location": st.map(os.path.realpath, config.data["location"])
#         }

#     @transition("DOWN", "UP")
#     def create(
#         self,
#         current: BoundState,
#         config: BoundState,
#         session: TaskSession,
#         input: Object,
#     ) -> Object:

#         result_path = session["create_dir"] << self.create_dir(input.location)
#         return st.struct["location":result_path] >> {
#             "location": st.map(os.path.realpath, config.data["location"])
#         }

#     @transition("UP", "DOWN")
#     def delete(
#         self,
#         current: BoundState,
#         config: BoundState,
#         session: TaskSession,
#         input: Object,
#     ) -> Object:

#         current_literal = current.literal(session.ns.registry)
#         return session["remove_dir"] << self.remove_dir(current_literal.location)


# Declaring global resources

file_resource = MachineResource("file", FileMachine)

# Resource state factory
File = file_resource.s


# directory_resource = MachineResource("directory", DirectoryMachine)

# # Resource state factory
# Directory = directory_resource.s


RESOURCES = [
    file_resource,
    # directory_resource
]


def register() -> None:
    """
	Register default resources in this module
	"""
    for resource in RESOURCES:
        st.registry.register_resource(resource)
